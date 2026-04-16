"""Модуль общих функций для микросервиса apifast."""

import asyncio
import random
from functools import wraps

from typing_extensions import Any, Awaitable, Callable, TypeVar

from .zena_logging import get_logger

T = TypeVar("T")

logger = get_logger()


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
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            log = get_logger()
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == retries:
                        log.exception(
                            "retry.exhausted",
                            func=func.__name__,
                            error=str(e),
                        )
                        raise
                    wait = (backoff**attempt) + random.uniform(0, jitter)
                    log.warning(
                        "retry.attempt",
                        func=func.__name__,
                        error=str(e),
                        attempt=attempt,
                        retries=retries,
                        wait_sec=round(wait, 1),
                    )
                    await asyncio.sleep(wait)

            raise RuntimeError(f"{func.__name__}: исчерпаны все попытки")

        return wrapper

    return decorator
