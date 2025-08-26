import calendar
import json
import sqlite3
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set

import feedparser
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from config import (DB_PATH, NIKTA_BASE_URL, DEFAULT_REQUEST_TIMEOUT_SECONDS, get_logger, DB_CLEANUP_DAYS, MAX_CONCURRENT_WORKERS)
from utils import APIError, retry_on_exception

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logger = get_logger(__name__)

# --- КОНФИГУРАЦИЯ УПРАВЛЕНИЯ СБОЯМИ RSS ---
RSS_FAILURE_THRESHOLD = 5  # Кол-во сбоев для временного отключения
RSS_COOLDOWN_HOURS = 24  # На сколько часов отключать сбойную ленту
RSS_NOTIFICATION_COOLDOWN_DAYS = 7  # Не отправлять новое уведомление для того же URL в течение 7 дней


class NewsFetcher:
    """
    Класс, инкапсулирующий логику сбора, фильтрации и сохранения новостей из RSS.
    Управляет жизненным циклом БД и состоянием RSS-лент.
    """

    def __init__(self):
        self.db_path = DB_PATH
        self._setup_database()

    def _cleanup_old_news(self):
        if DB_CLEANUP_DAYS <= 0:
            return
        try:
            with sqlite3.connect(self.db_path) as conn:
                cleanup_date = datetime.now(timezone.utc) - timedelta(days=DB_CLEANUP_DAYS)
                cursor = conn.cursor()
                # Удаляем только старые записи из таблицы news
                cursor.execute("DELETE FROM news WHERE published_dt < ?", (cleanup_date,))
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    logger.info("Очистка БД: удалено %d старых записей (старше %d дней).", deleted_count, DB_CLEANUP_DAYS)
        except sqlite3.Error as e:
            logger.error("Ошибка при очистке старых новостей из БД: %s", e)

    def _setup_database(self):
        self._cleanup_old_news()
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS news (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        country_code TEXT NOT NULL,
                        title TEXT NOT NULL,
                        summary TEXT,
                        link TEXT NOT NULL UNIQUE,
                        published_dt TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_published_dt ON news (published_dt);')

                # --- ИЗМЕНЕНО: Добавлены поля country_code и notification_sent_at ---
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS rss_feed_status (
                        url TEXT PRIMARY KEY,
                        country_code TEXT,
                        consecutive_failures INTEGER DEFAULT 0,
                        disabled_until TIMESTAMP,
                        last_error_message TEXT,
                        notification_sent_at TIMESTAMP
                    );
                ''')
                
                # Таблица для хранения времени последнего запуска
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS last_run_time (
                        id INTEGER PRIMARY KEY,
                        last_run_at TIMESTAMP NOT NULL
                    );
                ''')
                
                # Инициализируем первую запись, если таблица пустая
                cursor.execute('SELECT COUNT(*) FROM last_run_time')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('INSERT INTO last_run_time (last_run_at) VALUES (?)', 
                                 (datetime.now(timezone.utc).isoformat(),))
                
                conn.commit()
        except sqlite3.Error:
            logger.critical("Критическая ошибка при настройке базы данных", exc_info=True)
            raise

    # --- НОВЫЙ МЕТОД: Синхронизация БД с источником (Excel) ---
    def sync_feeds_from_source(self, all_source_urls: Set[str], all_source_feeds_by_country: dict):
        """
        Гарантирует, что таблица статусов лент соответствует файлу Excel.
        Удаляет устаревшие ленты и добавляет новые.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # 1. Получаем все ленты, которые сейчас отслеживаются в БД
                cursor.execute("SELECT url FROM rss_feed_status")
                db_urls = {row[0] for row in cursor.fetchall()}

                # 2. Находим ленты для удаления (есть в БД, но нет в Excel)
                urls_to_remove = db_urls - all_source_urls
                if urls_to_remove:
                    logger.info("Удаление %d устаревших лент из таблицы статусов.", len(urls_to_remove))
                    placeholders = ','.join('?' for _ in urls_to_remove)
                    conn.execute(f"DELETE FROM rss_feed_status WHERE url IN ({placeholders})", tuple(urls_to_remove))

                # 3. Добавляем новые ленты и обновляем коды стран для существующих
                for country, urls in all_source_feeds_by_country.items():
                    for url in urls:
                        conn.execute(
                            """
                            INSERT INTO rss_feed_status (url, country_code) VALUES (?, ?)
                            ON CONFLICT(url) DO UPDATE SET country_code=excluded.country_code
                            """,
                            (url, country)
                        )
                conn.commit()
        except sqlite3.Error:
            logger.error("Не удалось синхронизировать БД статусов лент с файлом-источником.", exc_info=True)

    def get_last_run_time(self) -> datetime:
        """Получает время последнего запуска из базы данных."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT last_run_at FROM last_run_time ORDER BY id DESC LIMIT 1")
                result = cursor.fetchone()
                if result:
                    return datetime.fromisoformat(result[0])
                else:
                    # Если записи нет, возвращаем время "час назад" для обратной совместимости
                    return datetime.now(timezone.utc) - timedelta(hours=1)
        except sqlite3.Error:
            logger.error("Ошибка при получении времени последнего запуска из БД", exc_info=True)
            # В случае ошибки возвращаем время "час назад"
            return datetime.now(timezone.utc) - timedelta(hours=1)

    def update_last_run_time(self, run_time: datetime):
        """Обновляет время последнего запуска в базе данных."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Обновляем единственную запись в таблице
                cursor.execute("UPDATE last_run_time SET last_run_at = ? WHERE id = (SELECT MIN(id) FROM last_run_time)", 
                             (run_time.isoformat(),))
                # Если по какой-то причине записи нет, создаем ее
                if cursor.rowcount == 0:
                    cursor.execute("INSERT INTO last_run_time (last_run_at) VALUES (?)", 
                                 (run_time.isoformat(),))
                conn.commit()
                logger.info("Время последнего запуска обновлено: %s", run_time.isoformat())
        except sqlite3.Error:
            logger.error("Ошибка при обновлении времени последнего запуска в БД", exc_info=True)

    def _get_active_feeds(self, feed_urls: List[str]) -> List[str]:
        """Фильтрует список URL, оставляя только активные (не отключенные) ленты."""
        active_feeds = []
        now_utc = datetime.now(timezone.utc)
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                placeholders = ','.join('?' for _ in feed_urls)
                cursor.execute(f"SELECT url, disabled_until FROM rss_feed_status WHERE url IN ({placeholders})",
                               feed_urls)

                disabled_feeds = {}
                for url, disabled_until_str in cursor.fetchall():
                    if disabled_until_str:
                        disabled_feeds[url] = datetime.fromisoformat(disabled_until_str)

                for url in feed_urls:
                    if url in disabled_feeds and now_utc < disabled_feeds[url]:
                        logger.warning("RSS-лента %s временно отключена до %s. Пропускаем.", url,
                                       disabled_feeds[url].isoformat())
                        continue
                    active_feeds.append(url)
        except sqlite3.Error:
            logger.error("Ошибка при проверке статуса RSS-лент. Будут опрошены все ленты.", exc_info=True)
            return feed_urls
        return active_feeds

    def _record_feed_success(self, url: str):
        """Сбрасывает счетчик ошибок для успешной ленты."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE rss_feed_status SET consecutive_failures = 0, disabled_until = NULL, last_error_message = NULL WHERE url = ?",
                    (url,)
                )
        except sqlite3.Error:
            logger.error("Не удалось обновить статус успеха для RSS-ленты %s", url, exc_info=True)

    # --- ИЗМЕНЕНО: Записывает сбой и возвращает информацию для Excel ---
    def _record_feed_failure(self, url: str, error_message: str) -> Tuple[str, Optional[str]]:
        """
        Записывает сбой, отключает ленту при необходимости и возвращает кортеж для отчетов.
        Возвращает: (отчет_о_сбое_для_временных_сбоев, сообщение_для_отключенных_лент)
        """
        alert_message = None
        now_utc = datetime.now(timezone.utc)
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT consecutive_failures, notification_sent_at, country_code FROM rss_feed_status WHERE url = ?",
                    (url,)
                )
                result = cursor.fetchone()
                if not result:  # Если ленты нет в статусах, значит, sync не сработал
                    logger.error("Не удалось найти статус для URL: %s. Пропускаем запись ошибки.", url)
                    return f"`{url}`\n  └─ {error_message} (ошибка: статус не найден в БД)", None

                failures = result[0] + 1
                last_notification = datetime.fromisoformat(result[1]) if result[1] else None
                country_code = result[2]

                if failures >= RSS_FAILURE_THRESHOLD:
                    disabled_until = now_utc + timedelta(hours=RSS_COOLDOWN_HOURS)
                    # Проверяем, нужно ли отправлять уведомление
                    if not last_notification or (now_utc - last_notification) > timedelta(
                            days=RSS_NOTIFICATION_COOLDOWN_DAYS):
                        conn.execute(
                            "UPDATE rss_feed_status SET consecutive_failures=?, disabled_until=?, last_error_message=?, notification_sent_at=? WHERE url=?",
                            (failures, disabled_until.isoformat(), error_message, now_utc.isoformat(), url)
                        )
                        disable_msg = f"Лента отключена на {RSS_COOLDOWN_HOURS} часов после {failures} сбоев."
                        logger.error("RSS-лента %s отключена. Будет добавлена в список отключенных. Причина: %s", url,
                                     error_message)
                        alert_message = f"Страна: {country_code}\nURL: {url}\nПричина: {error_message}"
                    else:  # Лента отключается, но уведомление уже недавно отправлялось
                        conn.execute(
                            "UPDATE rss_feed_status SET consecutive_failures=?, disabled_until=?, last_error_message=? WHERE url=?",
                            (failures, disabled_until.isoformat(), error_message, url)
                        )
                else:
                    conn.execute("UPDATE rss_feed_status SET consecutive_failures=?, last_error_message=? WHERE url=?",
                                 (failures, error_message, url))

                failure_report = f"<code>{url}</code>\n  └─ {error_message} (сбой #{failures})"
                return failure_report, alert_message
        except sqlite3.Error:
            logger.error("Не удалось обновить статус сбоя для RSS-ленты %s", url, exc_info=True)
            return f"<code>{url}</code>\n  └─ {error_message} (ошибка записи в БД)", None

    def _fetch_single_rss(self, url: str, country_code: str, start_dt_utc: datetime, end_dt_utc: datetime) -> Tuple[
        List[tuple], Optional[str], Optional[str]]:
        """Загружает одну ленту. Возвращает (новости, отчет_об_ошибке, уведомление)."""
        log_ctx = {'source': 'RSS', 'country_code': country_code, 'url': url}
        try:
            response = requests.get(url, headers={'User-Agent': 'MyNewsBot/1.0'}, timeout=(5, 10))
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            if feed.bozo:
                raise ValueError(f"Ошибка парсинга RSS: {feed.get('bozo_exception', 'Неизвестная ошибка')}")

            news_for_feed = []
            for entry in feed.entries:
                if not getattr(entry, 'published_parsed', None): continue
                pub_dt = datetime.fromtimestamp(calendar.timegm(entry.published_parsed), tz=timezone.utc)
                if start_dt_utc <= pub_dt <= end_dt_utc:
                    news_for_feed.append((
                        country_code, entry.title.strip(),
                        getattr(entry, 'summary', '').strip(), entry.link.strip(), pub_dt
                    ))
            self._record_feed_success(url)
            return news_for_feed, None, None
        except (requests.exceptions.RequestException, ValueError) as e:
            failure_report, alert = self._record_feed_failure(url, str(e))
            return [], failure_report, alert
        except Exception:
            logger.error("Непредвиденная ошибка при обработке ленты %s", url, extra={'context': log_ctx}, exc_info=True)
            failure_report, alert = self._record_feed_failure(url, "Непредвиденная ошибка (см. логи)")
            return [], failure_report, alert

    def _fetch_from_rss(self, country_code: str, feed_urls: List[str], start_dt_utc: datetime, end_dt_utc: datetime) -> \
    Tuple[List[tuple], List[str], List[str]]:
        news_candidates = []
        failures = []
        alerts = []

        active_urls = self._get_active_feeds(feed_urls)
        if len(active_urls) < len(feed_urls):
            logger.info("Будет опрошено %d из %d RSS-лент для страны %s (остальные временно отключены).",
                        len(active_urls), len(feed_urls), country_code)

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
            future_to_url = {executor.submit(self._fetch_single_rss, url, country_code, start_dt_utc, end_dt_utc): url
                             for url in active_urls}
            for future in future_to_url:
                try:
                    news_items, error_report, alert_msg = future.result()
                    if error_report: failures.append(error_report)
                    if alert_msg: alerts.append(alert_msg)
                    if news_items: news_candidates.extend(news_items)
                except Exception as exc:
                    url = future_to_url[future]
                    logger.error("Критическая ошибка в потоке для URL %s: %s", url, exc)
                    failures.append(f"<code>{url}</code>\n  └─ Внутренняя ошибка: {exc}")

        return news_candidates, failures, alerts

    def _get_new_unique_news(self, news_candidates: List[tuple], country_code: str) -> List[tuple]:
        if not news_candidates: return []
        links_to_check = [item[3] for item in news_candidates]
        existing_links = set()
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                CHUNK_SIZE = 900
                for i in range(0, len(links_to_check), CHUNK_SIZE):
                    chunk = links_to_check[i:i + CHUNK_SIZE]
                    placeholders = ','.join('?' for _ in chunk)
                    query = f'SELECT link FROM news WHERE link IN ({placeholders})'
                    cursor.execute(query, chunk)
                    existing_links.update(row[0] for row in cursor.fetchall())
            new_news = [item for item in news_candidates if item[3] not in existing_links]
            logger.info(
                f"Фильтрация дубликатов для {country_code}: {len(news_candidates)} -> {len(new_news)} новых новостей.")
            return new_news
        except sqlite3.Error:
            logger.error("Ошибка при проверке дубликатов для %s. Обработка продолжится без фильтрации.", country_code,
                         exc_info=True)
            return news_candidates

    def _store_news(self, news_to_store: List[tuple], country_code: str) -> int:
        if not news_to_store: return 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    'INSERT OR IGNORE INTO news (country_code, title, summary, link, published_dt) VALUES (?, ?, ?, ?, ?)',
                    news_to_store)
                conn.commit()
                return cursor.rowcount
        except sqlite3.Error:
            logger.error("Ошибка при записи новостей для %s.", country_code, exc_info=True)
            return 0

    def fetch_and_process_news(self, country_code: str, rss_links: List[str], start_dt_utc, end_dt_utc) -> Tuple[
        str, List[str], List[str]]:
        logger.info("Начинаем сбор новостей из RSS для страны: %s", country_code)
        rss_news, fetch_failures, fetch_alerts = self._fetch_from_rss(country_code, rss_links, start_dt_utc, end_dt_utc)
        logger.info(f"Найдено {len(rss_news)} новостей-кандидатов в RSS для {country_code}.")

        if not rss_news and not fetch_failures and not fetch_alerts:
            return "", [], []

        new_unique_news = self._get_new_unique_news(rss_news, country_code)
        if not new_unique_news:
            return "", fetch_failures, fetch_alerts

        added_count = self._store_news(new_unique_news, country_code)
        logger.info(f"Успешно добавлено {added_count} новых записей в БД для {country_code}.")

        # ... (здесь мы не формируем текст, это можно перенести в main.py, если нужно) ...
        # Для простоты, оставим формирование отчета здесь.
        report_lines = [
            f"<b>{item[1]}</b>\n<i>Published: {item[4].strftime('%Y-%m-%d')}</i>\n{item[2]}\n<a href='{item[3]}'>Source</a>"
            for item in new_unique_news
        ]
        report_text = "\n\n".join(report_lines)
        return report_text, fetch_failures, fetch_alerts


# ... (остальная часть файла NiktaAPIClient без изменений) ...

class NiktaAPIClient:
    """Клиент для взаимодействия с Nikta LLM API с retry-логикой."""

    def __init__(self, email: str, password: str):
        self.base_url = NIKTA_BASE_URL
        self._email = email
        self._password = password
        self.session = requests.Session()
        self.session.verify = False
        self.session.timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS

    @retry_on_exception(exceptions=(APIError, requests.RequestException))
    def authenticate(self):
        log_ctx = {'api': 'Nikta', 'operation': 'authenticate'}
        logger.info("Аутентификация...", extra={'context': log_ctx})
        payload = {"email": self._email, "password": self._password}
        try:
            response = self.session.post(f"{self.base_url}/login", json=payload)
            response.raise_for_status()
            token = response.json().get("token")
            if not token:
                raise APIError("Токен не найден в ответе сервера.")
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info("Аутентификация прошла успешно.", extra={'context': log_ctx})
        except requests.RequestException as e:
            raise APIError(f"Сетевая ошибка при аутентификации: {e}")
        except (json.JSONDecodeError, KeyError) as e:
            raise APIError(f"Ошибка парсинга ответа при аутентификации: {e}")

    @retry_on_exception(exceptions=(APIError, requests.RequestException))
    def run_scenario(self, scenario_id: int, message: str, info: dict) -> dict:
        log_ctx = {'api': 'Nikta', 'scenario_id': scenario_id}
        if "Authorization" not in self.session.headers:
            raise APIError("Клиент не аутентифицирован.")

        logger.info("Запуск сценария...", extra={'context': log_ctx})
        payload = {
            "scenario_id": scenario_id, "channel_id": '1', "dialog_id": '1', "user_id": 1,
            "state": {"messages": [{"role": "human", "content": message}], "info": info}
        }
        try:
            response = self.session.post(f"{self.base_url}/run", json=payload)
            response.raise_for_status()
            logger.info("Сценарий успешно выполнен.", extra={'context': log_ctx})
            return response.json()
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Ошибка 401. JWT токен, вероятно, истек. Требуется повторная аутентификация.",
                             extra={'context': log_ctx})
                self.authenticate()
            raise APIError(f"HTTP ошибка: {e.response.status_code} {e.response.text}")
        except requests.RequestException as e:
            raise APIError(f"Сетевая ошибка: {e}")
        except json.JSONDecodeError:
            raise APIError(f"Не удалось декодировать JSON из ответа: {response.text}")