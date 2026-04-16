"""Функции для работы с API httpservice.ai2b.pro."""

import asyncio
import random

import aiohttp
from typing_extensions import Any, Awaitable, Callable, Type, TypeVar

from ..zena_logging import get_logger

T = TypeVar("T")

logger = get_logger()

async def sent_message_to_history(
    user_id: int,
    text: str,
    user_companychat: int,
    reply_to_history_id: int,
    access_token: str,
    tokens: dict[str, Any],
    tools: list[str],
    tools_args: dict[str, Any],
    tools_result: dict[str, Any],
    prompt_system: str,
    template_prompt_system: str,
    dialog_state: str,
    dialog_state_new: str,
) -> dict[str, Any]:
    """Отправка переменных на endpoint для сохранения с повтором при ошибках."""
    return await retry_async(
        _sent_message_to_history,
        user_id,
        text,
        user_companychat,
        reply_to_history_id,
        access_token,
        tokens,
        tools,
        tools_args,
        tools_result,
        prompt_system,
        template_prompt_system,
        dialog_state,
        dialog_state_new,
    )


async def _sent_message_to_history(
    user_id: int,
    text: str,
    user_companychat: int,
    reply_to_history_id: int,
    access_token: str,
    tokens: dict[str, Any],
    tools: list[str],
    tools_args: dict[str, Any],
    tools_result: dict[str, Any],
    prompt_system: str,
    template_prompt_system: str,
    dialog_state: str,
    dialog_state_new: str,
) -> dict[str, Any]:
    """Отправка переменных на endpoint для сохранения."""
    url = "https://httpservice.ai2b.pro/v1/telegram/n8n/outgoing"
    payload = {
        "user_id": user_id,
        "text": text,
        "user_companychat": user_companychat,
        "reply_to_history_id": 6800, #reply_to_history_id,
        "access_token": access_token,
        "tokens": tokens,
        "tools": tools,
        "tools_args": tools_args,
        "tools_result": tools_result,
        "prompt_system": prompt_system,
        "template_prompt_system": template_prompt_system,
        "dialog_state": dialog_state,
        "dialog_state_new": dialog_state_new,
    }
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            logger.info("http.send_message", user_cc=user_companychat)
            async with session.post(url, json=payload) as resp:
                resp.raise_for_status()
                return await resp.json()
    except aiohttp.ClientResponseError as e:
        logger.warning("http.error", status=e.status, message=e.message)
        raise
    except (aiohttp.ConnectionTimeoutError, aiohttp.ServerTimeoutError):
        logger.warning("http.timeout")
        raise
    except aiohttp.ClientError as e:
        logger.warning("http.client_error", error=str(e))
        raise


async def retry_async(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    retries: int = 1,
    backoff: float = 2.0,
    jitter: float = 1.0,
    exceptions: tuple[Type[BaseException], ...] = (Exception,),
    **kwargs: Any,
) -> T:
    """Асинхронные ретраи с экспоненциальным бэкоффом и равномерным джиттером.

    - func: async-функция, которую ретраим
    - retries: общее число попыток
    - backoff: базовый коэффициент экспоненты (например, 2.0 => 2^attempt)
    - jitter: амплитуда добавочного шума [0, jitter)
    - exceptions: кортеж типов исключений, которые нужно ретраить
    """
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions as e:
            if attempt == retries:
                logger.exception(
                    "retry.exhausted",
                    func=getattr(func, "__name__", str(func)),
                    error=str(e),
                )
                raise
            wait = (backoff**attempt) + random.uniform(0, jitter)
            logger.warning(
                "retry.attempt",
                func=getattr(func, "__name__", str(func)),
                error=str(e),
                attempt=attempt,
                retries=retries,
                wait_sec=round(wait, 1),
            )
            # Неблокирующее ожидание — не мешает другим корутинам
            await asyncio.sleep(wait)
    # Этот raise теоретически невозможен, но для mypy необходим
    raise RuntimeError("retry_async exhausted all retries without returning")
