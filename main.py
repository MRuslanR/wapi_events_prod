# main.py
import sqlite3
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Set
import html

from config import (ConfigError, NIKTA_USER_EMAIL, NIKTA_USER_PASSWORD,
                    get_logger, load_feed_sources, NIKTA_FILTER_SCENARIO_ID,
                    NIKTA_FINAL_REPORT_SCENARIO_ID, N_HOURS, MAX_CONCURRENT_WORKERS, MAX_PARSE_HOURS)
from services import NiktaAPIClient, NewsFetcher
from utils import APIError

sqlite3.register_adapter(datetime, lambda ts: ts.isoformat())
sqlite3.register_converter("timestamp", lambda val: datetime.fromisoformat(val.decode()))

logger = get_logger(__name__)


def escape_html(text: str) -> str:
    return html.escape(str(text))


def process_country(
        country_code: str,
        rss_links: List[str],
        news_fetcher: NewsFetcher,
        nikta_client: NiktaAPIClient,
        start_dt_utc: datetime,
        end_dt_utc: datetime
) -> Tuple[str, str, int, float, List[str], List[str]]:
    """
    Выполняет полный цикл обработки для одной страны.
    Возвращает: (код страны, отчет, токены, цена, ошибки RSS, уведомления RSS).
    """
    log_ctx = {'context': {'country_code': country_code}}
    result = None
    try:
        raw_news_text, fetch_failures, fetch_alerts = news_fetcher.fetch_and_process_news(
            country_code=country_code, rss_links=rss_links,
            start_dt_utc=start_dt_utc, end_dt_utc=end_dt_utc
        )

        if not raw_news_text.strip():
            logger.info("Нет новых новостей для обработки Nikta для %s.", country_code, extra=log_ctx)
            return country_code, "", 0, 0.0, fetch_failures, fetch_alerts

        result = nikta_client.run_scenario(NIKTA_FILTER_SCENARIO_ID, raw_news_text, {})
        nikta_tokens = result.get('tokens', 0)
        nikta_price = result.get('logs', {}).get('total_price', 0.0)
        filtered_report = result.get('result', "")

        logger.info(
            "Новости для %s отфильтрованы. Потрачено токенов Nikta: %d, цена: $%.4f",
            country_code, nikta_tokens, nikta_price, extra=log_ctx
        )
        return country_code, filtered_report, nikta_tokens, nikta_price, fetch_failures, fetch_alerts
    except APIError as e:
        logger.error("Ошибка API при обработке страны %s: %s", country_code, e, extra=log_ctx)
    except Exception:
        api_response_info = f", ответ от API: {result}" if result else ""
        logger.error(f"Непредвиденная ошибка при обработке страны %s{api_response_info}", country_code, extra=log_ctx,
                     exc_info=True)
    return country_code, "", 0, 0.0, [], []


def main_cycle() -> Tuple[str, str]:
    """
    Основная функция. Возвращает кортеж: (основное_сообщение_для_дайджеста, сообщение_уведомлений_для_менеджеров)
    """
    logger.info("--- Начало нового цикла обработки ---")

    try:
        feed_sources = load_feed_sources()
        all_source_urls: Set[str] = {url for urls in feed_sources.values() for url in urls}
    except ConfigError as e:
        logger.critical("Критическая ошибка конфигурации: %s.", e, exc_info=True)
        return f"<b>Ошибка конфигурации:</b>\n<code>{escape_html(e)}</code>", ""

    if not feed_sources:
        logger.warning("Источники новостей не настроены. Пропуск цикла.")
        return "⚠️ <b>Источники новостей (RSS) не настроены.</b>\nДобавьте их в <code>config.xlsx</code>.", ""

    try:
        nikta_client = NiktaAPIClient(NIKTA_USER_EMAIL, NIKTA_USER_PASSWORD)
        nikta_client.authenticate()
        news_fetcher = NewsFetcher()
        logger.info("Синхронизация лент из источника с базой данных...")
        news_fetcher.sync_feeds_from_source(all_source_urls, feed_sources)
    except (APIError, sqlite3.Error) as e:
        logger.critical("Не удалось инициализировать клиенты или БД: %s.", e, exc_info=True)
        return f"<b>Критическая ошибка инициализации:</b>\n<code>{escape_html(e)}</code>", ""

    # Новая логика определения временного окна
    end_dt_utc = datetime.now(timezone.utc)
    last_run_time = news_fetcher.get_last_run_time()
    
    # Вычисляем время с последнего запуска
    time_since_last_run = end_dt_utc - last_run_time
    hours_since_last_run = time_since_last_run.total_seconds() / 3600
    
    # Ограничиваем максимальным временем парсинга
    if hours_since_last_run > MAX_PARSE_HOURS:
        start_dt_utc = end_dt_utc - timedelta(hours=MAX_PARSE_HOURS)
        logger.info("Время с последнего запуска (%.1f ч) превышает максимум (%d ч). Ограничиваем окно поиска.", 
                   hours_since_last_run, MAX_PARSE_HOURS)
    else:
        start_dt_utc = last_run_time
        logger.info("Парсим с времени последнего запуска (%.1f ч назад).", hours_since_last_run)
    
    logger.info("Временное окно для поиска: с %s по %s.", start_dt_utc.isoformat(), end_dt_utc.isoformat())

    filtered_reports: Dict[str, str] = {}
    total_nikta_tokens: int = 0
    total_nikta_price: float = 0.0
    all_failures: Dict[str, List[str]] = {}
    all_alerts: List[str] = []

    num_countries = len(feed_sources)
    workers = min(num_countries, MAX_CONCURRENT_WORKERS) if num_countries > 0 else 1
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_country = {
            executor.submit(process_country, country, rss_links, news_fetcher, nikta_client, start_dt_utc,
                            end_dt_utc): country
            for country, rss_links in feed_sources.items()
        }
        for future in as_completed(future_to_country):
            country = future_to_country[future]
            try:
                _country, report, tokens, price, failures, alerts = future.result()
                if report: filtered_reports[_country] = report
                total_nikta_tokens += tokens
                total_nikta_price += price
                if failures: all_failures[_country] = failures
                if alerts: all_alerts.extend(alerts)
            except Exception as exc:
                logger.error("Ошибка при получении результата для страны '%s': %s", country, exc, exc_info=True)

    final_report_text = "\n\n<hr>\n\n".join(
        [f"<b>Новости для {country}:</b>\n{report}" for country, report in filtered_reports.items() if report.strip()]
    )

    final_digest = ""
    if final_report_text:
        logger.info("Отправка для создания итогового дайджеста в Nikta...")
        try:
            result = nikta_client.run_scenario(NIKTA_FINAL_REPORT_SCENARIO_ID, final_report_text, {})
            final_tokens = result.get('tokens', 0)
            total_nikta_tokens += final_tokens
            final_price = result.get('logs', {}).get('total_price', 0.0)
            total_nikta_price += final_price
            final_digest = result.get('result', "")
            logger.info(
                "Итоговый дайджест успешно создан. Потрачено токенов: %d, цена: $%.4f",
                final_tokens, final_price
            )
        except APIError as e:
            logger.error("Не удалось создать итоговый дайджест через Nikta.", exc_info=True)
            final_digest = f"⚠️ <b>Не удалось сгенерировать финальный дайджест:</b> <code>{escape_html(e)}</code>"

    # --- ИТОГОВАЯ СТАТИСТИКА ЦИКЛА (ТОЛЬКО ДЛЯ ЛОГОВ) ---
    nikta_stats = f"Nikta (всего): {total_nikta_tokens} токенов, ${total_nikta_price:.5f}"
    logger.info("Итоговая статистика цикла: %s", nikta_stats)

    # --- СОХРАНЕНИЕ ИНФОРМАЦИИ О СБОЯХ В EXCEL ---
    if all_failures or all_alerts:
        logger.info("Сохранение информации о сбоях RSS в Excel...")
        try:
            from config import update_excel_with_failures
            update_excel_with_failures(all_failures, all_alerts)
        except Exception as e:
            logger.error("Ошибка при сохранении сбоев в Excel: %s", e, exc_info=True)

    # Основное сообщение (только дайджест)
    main_message = final_digest if final_digest else "No news"

    # Обновляем время последнего запуска в базе
    try:
        news_fetcher.update_last_run_time(end_dt_utc)
    except Exception as e:
        logger.error("Ошибка при обновлении времени последнего запуска: %s", e)

    logger.info("--- Цикл обработки завершен ---")
    return main_message, ""