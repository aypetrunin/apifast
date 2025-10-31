"""Модуль общих функций для микросервиса apifast."""

import asyncio
import logging
import random
from functools import wraps

from typing_extensions import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")

# -------------------- Logging --------------------
# Настройка логирования для вывода сообщений в консоль
logging.basicConfig(
    level=logging.INFO,  # минимальный уровень логирования INFO
    format="%(asctime)s [%(levelname)s] %(message)s",  # формат: время [уровень] сообщение
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)  # создаём логгер для текущего модуля


def retry_async(
    retries: int = 3,
    backoff: float = 2.0,
    jitter: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Декоратор для асинхронных ретраев с экспоненциальным бэкоффом и равномерным джиттером.

    Args:
        retries: общее число попыток (по умолчанию 3)
        backoff: базовый коэффициент экспоненты (например, 2.0 => 2^attempt)
        jitter: амплитуда добавочного шума [0, jitter)
        exceptions: кортеж типов исключений, которые нужно ретраить

    Example:
        @retry_async()
        async def fetch_data(conn, user_id):
            return await conn.fetchrow(...)

        @retry_async(retries=5, backoff=1.5, exceptions=(asyncpg.TimeoutError,))
        async def fetch_critical_data(conn, user_id):
            return await conn.fetchrow(...)
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == retries:
                        logger.exception(
                            f"Последняя неудачная попытка {func.__name__}: {e}"
                        )
                        raise
                    wait = (backoff**attempt) + random.uniform(0, jitter)
                    logger.warning(
                        f"Ошибка в {func.__name__}: {e} | "
                        f"попытка {attempt}/{retries} — повтор через {wait:.1f}s"
                    )
                    # Неблокирующее ожидание — не мешает другим корутинам
                    await asyncio.sleep(wait)

            # Эта строка никогда не должна быть достигнута
            raise RuntimeError(f"{func.__name__}: исчерпаны все попытки")

        return wrapper

    return decorator
