import time
import random
from functools import wraps
from typing import Callable, Any

from config import get_logger

logger = get_logger(__name__)


class APIError(Exception):
    """Кастомное исключение для ошибок при работе с внешними API."""
    pass


def retry_on_exception(
        tries: int = 3,
        delay_seconds: int = 5,
        backoff_factor: int = 2,
        exceptions: tuple = (APIError,)
) -> Callable:
    """
    Декоратор для повторного вызова функции при возникновении исключений.

    :param tries: Максимальное количество попыток.
    :param delay_seconds: Начальная задержка между попытками.
    :param backoff_factor: Множитель для увеличения задержки (экспоненциальная задержка).
    :param exceptions: Кортеж исключений, при которых нужно повторять попытку.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay_seconds
            for i in range(1, tries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if i == tries:
                        logger.error(
                            "Последняя попытка (%d/%d) не удалась. Исключение: %s",
                            i, tries, e, exc_info=True
                        )
                        raise

                    # Добавляем "jitter" (дрожание) к задержке, чтобы избежать "громовых стад"
                    jitter = current_delay * random.uniform(0.1, 0.5)
                    wait_time = current_delay + jitter

                    logger.warning(
                        "Попытка %d/%d не удалась. Ошибка: %s. Повтор через %.2f секунд...",
                        i, tries, e, wait_time
                    )
                    time.sleep(wait_time)
                    current_delay *= backoff_factor

        return wrapper

    return decorator