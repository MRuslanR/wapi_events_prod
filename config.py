import logging
import sys
from pathlib import Path
from typing import Dict, List
import re
from datetime import time, datetime

import pandas as pd
from dotenv import load_dotenv
import os

# --- Загрузка переменных окружения ---
load_dotenv()

# --- Константы для Telegram ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID_STR = os.environ.get("TELEGRAM_CHAT_ID")
try:
    TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID_STR) if TELEGRAM_CHAT_ID_STR else None
except (ValueError, TypeError):
    TELEGRAM_CHAT_ID = None

# Пути
BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILEPATH = BASE_DIR / 'config.xlsx'
DB_PATH = BASE_DIR / 'data' / 'news_database.db'

# Nikta API
NIKTA_BASE_URL = "https://wapi.nikta.ai/llm/api"
NIKTA_USER_EMAIL = os.environ.get("NIKTA_USER_EMAIL")
NIKTA_USER_PASSWORD = os.environ.get("NIKTA_USER_PASSWORD")
NIKTA_FILTER_SCENARIO_ID = 4
NIKTA_FINAL_REPORT_SCENARIO_ID = 2

# Настройки по умолчанию
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10
DEFAULT_N_HOURS = 1
DEFAULT_MAX_PARSE_HOURS = 8  # Максимальное время парсинга RSS лент
# Константа для очистки БД - если < 0, то не будет чиститься
DB_CLEANUP_DAYS = 3
MAX_CONCURRENT_WORKERS = 8


# Чтение конфигурационных параметров из .env
try:
    N_HOURS = int(os.environ.get("N_HOURS", DEFAULT_N_HOURS))
except (ValueError, TypeError):
    N_HOURS = DEFAULT_N_HOURS

try:
    MAX_PARSE_HOURS = int(os.environ.get("MAX_PARSE_HOURS", DEFAULT_MAX_PARSE_HOURS))
except (ValueError, TypeError):
    MAX_PARSE_HOURS = DEFAULT_MAX_PARSE_HOURS


class ConfigError(Exception):
    """Кастомное исключение для ошибок конфигурации."""
    pass


### Централизованная настройка логирования ###

class ContextFilter(logging.Filter):
    """
    Пользовательский фильтр для добавления контекста в логи.
    """

    def filter(self, record):
        if not hasattr(record, 'context'):
            record.context = ''
        elif isinstance(record.context, dict) and record.context:
            record.context = ", ".join(f"{k}={v}" for k, v in record.context.items())
        else:
            record.context = str(record.context)
        return True


def setup_logging():
    """
    Настраивает корневой логгер для всего приложения.
    Должна вызываться один раз при старте.
    """
    # Убираем все существующие обработчики с корневого логгера, чтобы избежать дублирования
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] - [%(funcName)s] - [%(context)s] - %(message)s'
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(ContextFilter())

    # Настраиваем корневой логгер
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    # Устанавливаем более низкий уровень для логов библиотеки telegram, чтобы не засорять вывод
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Возвращает экземпляр логгера с указанным именем.
    """
    return logging.getLogger(name)


logger = get_logger(__name__)


def load_feed_sources() -> Dict[str, List[str]]:
    """Загружает источники RSS-лент из листа 'Feeds' в Excel-файле."""
    logger.info("Загрузка источников RSS из '%s'", CONFIG_FILEPATH)
    if not NIKTA_USER_EMAIL or not NIKTA_USER_PASSWORD:
        raise ConfigError("Переменные окружения NIKTA_USER_EMAIL или NIKTA_USER_PASSWORD не установлены.")
    if not TELEGRAM_BOT_TOKEN:
        raise ConfigError("Переменная окружения TELEGRAM_BOT_TOKEN не установлена.")
    if not TELEGRAM_CHAT_ID:
        raise ConfigError("Переменная окружения TELEGRAM_CHAT_ID не установлена.")

    try:
        df = pd.read_excel(CONFIG_FILEPATH, sheet_name='Feeds')
        sources = {}
        for _, row in df.iterrows():
            country_code = row.get('country_code')
            if not isinstance(country_code, str) or not country_code.strip():
                continue
            urls = [url for url in row[1:].values if isinstance(url, str) and url.strip()]
            if urls:
                sources[country_code.strip()] = urls

        if not sources:
            logger.warning("Не найдено ни одного источника RSS-лент в конфигурационном файле.")
        else:
            logger.info("Источники RSS для стран %s успешно загружены.", list(sources.keys()))

        return sources
    except FileNotFoundError:
        raise ConfigError(f"Файл конфигурации '{CONFIG_FILEPATH}' не найден.")
    except Exception as e:
        logger.error("Не удалось прочитать источники RSS-лент из '%s'", CONFIG_FILEPATH, exc_info=True)
        raise ConfigError(f"Не удалось прочитать источники RSS-лент: {e}")


def load_schedule() -> List[str]:
    """
    Загружает расписание запусков из листа 'Schedule' в Excel-файле.
    обрабатывает время, прочитанное pandas как datetime.time (ЧЧ:ММ:СС)
    или как строку (ЧЧ:ММ).
    """
    logger.info("Загрузка расписания из '%s'", CONFIG_FILEPATH)
    time_format_regex = re.compile(r'^([01]\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?$')
    try:
        df = pd.read_excel(CONFIG_FILEPATH, sheet_name='Schedule')
        if 'run_time_utc' not in df.columns:
            logger.warning("В листе 'Schedule' отсутствует столбец 'run_time_utc'. Расписание не будет загружено.")
            return []

        times = []
        for item in df['run_time_utc'].dropna():
            # Случай 1: Pandas прочитал ячейку как объект datetime.time
            if isinstance(item, time):
                # Форматируем в нужный нам вид, отбрасывая секунды
                time_str = item.strftime('%H:%M')
                times.append(time_str)
                continue

            # Случай 2: Это не объект времени, обрабатываем как строку
            time_str = str(item).strip()
            if time_format_regex.match(time_str):
                # Приводим к единому формату ЧЧ:ММ, отрезая секунды, если они были
                times.append(time_str[:5])
            else:
                logger.warning(
                    "Неверный формат времени '%s' в расписании. Ожидается ЧЧ:ММ или ЧЧ:ММ:СС. Строка пропущена.",
                    time_str
                )

        # Убираем дубликаты и сортируем для порядка
        unique_times = sorted(list(set(times)))

        if not unique_times:
            logger.info("В файле конфигурации не найдено валидных времен для запуска по расписанию.")
        else:
            logger.info("Расписание успешно загружено. Времена запуска (UTC): %s", unique_times)

        return unique_times
    except FileNotFoundError:
        logger.warning("Файл конфигурации '%s' не найден. Запуск по расписанию невозможен.", CONFIG_FILEPATH)
        return []
    except Exception as e:
        # Если лист 'Schedule' отсутствует, pandas вызовет исключение, которое мы здесь обработаем
        if "No sheet named 'Schedule'" in str(e):
            logger.info("Лист 'Schedule' не найден в файле конфигурации. Запуск по расписанию не будет настроен.")
            return []
        logger.error("Не удалось прочитать расписание из '%s'", CONFIG_FILEPATH, exc_info=True)
        return []


def update_excel_with_failures(temp_failures: Dict[str, List[str]], disabled_feeds: List[str]):
    """
    Обновляет Excel файл с информацией о временных сбоях и отключенных RSS лентах.
    Создает листы 'TempFailures' и 'DisabledFeeds' если их нет.
    """
    logger.info("Обновление Excel файла с информацией о сбоях RSS")
    
    try:
        # Попытка загрузить существующие листы
        try:
            with pd.ExcelFile(CONFIG_FILEPATH) as xls:
                existing_sheets = xls.sheet_names
        except FileNotFoundError:
            existing_sheets = []
            logger.warning("Файл %s не найден, создаем новый", CONFIG_FILEPATH)
        
        # Подготовка данных для временных сбоев
        temp_failures_data = []
        current_time = datetime.now().isoformat()
        
        for country, failures in temp_failures.items():
            for failure in failures:
                # Извлекаем URL из HTML-тегов
                import re
                url_match = re.search(r'<code>(.+?)</code>', failure)
                url = url_match.group(1) if url_match else "Неизвестный URL"
                
                # Извлекаем описание ошибки
                error_match = re.search(r'└─ (.+?) \(сбой #\d+\)', failure)
                error_desc = error_match.group(1) if error_match else failure.replace('<code>', '').replace('</code>', '')
                
                temp_failures_data.append({
                    'Страна': country,
                    'RSS URL': url,
                    'Описание ошибки': error_desc,
                    'Время': current_time
                })
        
        # Подготовка данных для отключенных лент
        disabled_feeds_data = []
        
        for alert in disabled_feeds:
            # Парсим сообщение о отключенной ленте
            lines = alert.strip().split('\n')
            country = "Неизвестно"
            url = "Неизвестно"
            reason = "Неизвестно"
            
            for line in lines:
                if line.startswith('Страна:'):
                    country = line.replace('Страна:', '').strip()
                elif line.startswith('URL:'):
                    url = line.replace('URL:', '').strip()
                elif line.startswith('Причина:'):
                    reason = line.replace('Причина:', '').strip()
            
            disabled_feeds_data.append({
                'Страна': country,
                'RSS URL': url,
                'Причина отключения': reason,
                'Время отключения': current_time
            })
        
        # Сохранение в Excel
        with pd.ExcelWriter(CONFIG_FILEPATH, engine='openpyxl', mode='a' if CONFIG_FILEPATH.exists() else 'w', if_sheet_exists='replace') as writer:
            # Сохраняем существующие листы
            if CONFIG_FILEPATH.exists():
                try:
                    for sheet in existing_sheets:
                        if sheet not in ['TempFailures', 'DisabledFeeds']:
                            df = pd.read_excel(CONFIG_FILEPATH, sheet_name=sheet)
                            df.to_excel(writer, sheet_name=sheet, index=False)
                except Exception as e:
                    logger.warning("Ошибка при копировании существующих листов: %s", e)
            
            # Обновляем лист с временными сбоями
            if temp_failures_data:
                # Загружаем существующие данные если есть
                try:
                    existing_temp_df = pd.read_excel(CONFIG_FILEPATH, sheet_name='TempFailures')
                    temp_df = pd.concat([existing_temp_df, pd.DataFrame(temp_failures_data)], ignore_index=True)
                except:
                    temp_df = pd.DataFrame(temp_failures_data)
                
                temp_df.to_excel(writer, sheet_name='TempFailures', index=False)
                logger.info("Добавлено %d временных сбоев в Excel", len(temp_failures_data))
            
            # Обновляем лист с отключенными лентами
            if disabled_feeds_data:
                # Загружаем существующие данные если есть
                try:
                    existing_disabled_df = pd.read_excel(CONFIG_FILEPATH, sheet_name='DisabledFeeds')
                    disabled_df = pd.concat([existing_disabled_df, pd.DataFrame(disabled_feeds_data)], ignore_index=True)
                except:
                    disabled_df = pd.DataFrame(disabled_feeds_data)
                
                disabled_df.to_excel(writer, sheet_name='DisabledFeeds', index=False)
                logger.info("Добавлено %d отключенных лент в Excel", len(disabled_feeds_data))
        
        logger.info("Успешно обновлен файл %s", CONFIG_FILEPATH)
        
    except Exception as e:
        logger.error("Ошибка при обновлении Excel файла: %s", e, exc_info=True)
        raise