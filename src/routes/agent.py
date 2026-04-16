"""Модуль создания endpointa '/agent/run' - агента-бота."""

import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from langgraph_sdk.client import LangGraphClient
from langgraph_sdk.schema import Assistant

from ..deps import langgraph_client  # type: ignore
from ..schemas import AgentRunParams  # type: ignore
from ..zena_logging import bind_contextvars, clear_contextvars, get_logger, timed_block

logger = get_logger()

router = APIRouter(prefix="/agent", tags=["agent"])


# -----------------------------
# Helpers
# -----------------------------

def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _content_to_text(content: str | list[Any] | dict | None) -> str:
    """Нормализует content в строку (учитывает особенности LangGraph Studio)."""
    if isinstance(content, str):
        return content

    if isinstance(content, list) and content:
        part = content[0]
        if isinstance(part, dict):
            txt = part.get("text")
            if isinstance(txt, str):
                return txt
            cnt = part.get("content")
            if isinstance(cnt, str):
                return cnt

    if isinstance(content, dict) and content:
        cnt = content.get("content")
        if isinstance(cnt, str):
            return cnt

    return ""


async def _patch_thread_metadata(
    client: LangGraphClient,
    thread_id: str,
    patch: dict[str, Any],
) -> None:
    """
    Обновление метаданных threads.
    """
    if hasattr(client.threads, "update"):
        await client.threads.update(thread_id=thread_id, metadata=patch)
        return
    raise RuntimeError(
        "LangGraphClient.threads не поддерживает update/patch/set_metadata для metadata"
    )


async def _patch_user_meta(
    client: LangGraphClient,
    thread_id: str,
    user_companychat: str,
    delivery: dict[str, Any],
) -> None:
    await _patch_thread_metadata(
        client,
        thread_id,
        {
            "user_companychat": user_companychat,
            "last_user_ts": _utc_iso(),
            **delivery,

        },
    )


async def _patch_assistant_meta(
    client: LangGraphClient,
    thread_id: str,
    user_companychat: str,
    dialog_state: str,
    delivery: dict[str, Any],
) -> None:
    await _patch_thread_metadata(
        client,
        thread_id,
        {
            "user_companychat": user_companychat,
            "last_assistant_ts": _utc_iso(),
            "last_dialog_state": dialog_state or 'new',
            "reminded": 0, # обнуление счетчика возобновления диалога после ответа агента.
            **delivery,
        },
    )

# -----------------------------
# Endpoint
# -----------------------------

@router.post("/run")
async def run_sync(params: AgentRunParams) -> JSONResponse:
    text = ""
    delivery = {
        "delivery_user_id": int(params.user_id),
        "delivery_reply_to_history_id": int(params.reply_to_history_id),
        "delivery_access_token": str(params.access_token),
    }
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    success_response: dict[str, Any] = {"success": False, "exception": "unknown"}

    user_companychat = str(params.user_companychat)
    request_id = f"{user_companychat}:{int(time.time())}"
    clear_contextvars()
    bind_contextvars(user_cc=user_companychat, request_id=request_id)

    try:
        async with timed_block("agent.run"):
            logger.info("agent.run.started", assistant_id=params.assistant_id)
            logger.debug("agent.run.params", params=str(params))

            user_message = (params.message or "").strip()

            if not user_message:
                success_response = {"success": False, "exception": "empty message"}
                return JSONResponse(content=success_response, status_code=status.HTTP_400_BAD_REQUEST)

            async with langgraph_client() as client:
                assistant_id = await get_or_create_assistant(client, params)
                thread_id = await get_or_create_thread(client, assistant_id, params)

                # 1) фиксируем вход пользователя
                try:
                    await _patch_user_meta(client, thread_id, user_companychat, delivery)
                except Exception as e:
                    logger.warning("thread.patch_failed", thread_id=thread_id, patch="last_user_ts", error=str(e))

                # Инжектим request_id для сквозной трассировки
                if params.context is None:
                    params.context = {}
                params.context["_request_id"] = request_id

                run = await client.runs.create(
                    thread_id=thread_id,
                    assistant_id=assistant_id,
                    input={"messages": [{"role": "user", "content": user_message}]},
                    config=params.config,
                    context=params.context,
                    metadata=params.metadata,
                    on_completion="delete",
                )

                agent_response = await client.runs.join(
                    thread_id=run["thread_id"],
                    run_id=run["run_id"],
                )

                msgs = agent_response.get("messages")
                text = _content_to_text(msgs[-1])
                dialog_state = agent_response.get("data", {}).get("dialog_state")

                # 2) фиксируем время ответа ассистента и статус диалога
                try:
                    await _patch_assistant_meta(client, thread_id, user_companychat, dialog_state, delivery)
                except Exception as e:
                    logger.warning("thread.patch_failed", thread_id=thread_id, patch="last_assistant_ts", error=str(e))

            logger.info("agent.run.message_in", message=user_message)
            logger.info("agent.run.message_out", message=text)

            success_response = {"success": True, "exception": "no", "message": text}
            status_code = status.HTTP_200_OK

    except Exception as e:
        logger.exception("agent.run.failed", error=str(e))
        success_response = {"success": False, "exception": str(e)}
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(content=success_response, status_code=status_code)


# -----------------------------
# Assistant / Thread functions
# -----------------------------

async def get_or_create_assistant(client: LangGraphClient, params: AgentRunParams) -> str:
    user_companychat = str(params.user_companychat)

    assistants: list[Assistant] = await client.assistants.search(
        metadata={"user_companychat": user_companychat}
    )

    if assistants:
        logger.info("assistant.found", assistant_id=assistants[0]["assistant_id"])
        return assistants[0]["assistant_id"]

    assistant: Assistant = await client.assistants.create(
        name=f"user_{user_companychat}",
        context=params.context,
        graph_id=params.assistant_id,
        metadata={"user_companychat": user_companychat},
    )
    logger.info("assistant.created", assistant_id=assistant["assistant_id"])
    return assistant["assistant_id"]


async def get_or_create_thread(client: LangGraphClient, assistant_id: str, params: AgentRunParams) -> str:
    last_message = (params.message or "").strip().lower()
    user_companychat = str(params.user_companychat)

    if last_message == "стоп":
        logger.info("dialog.stop_command", user_cc=user_companychat)
        await _delete_thread(client, user_companychat)
        return await _create_thread(client, assistant_id, params)

    threads = await client.threads.search(
        metadata={"user_companychat": user_companychat},
        sort_by="created_at",
        sort_order="desc",
        limit=1,
    )

    if threads:
        logger.info("thread.found", thread_id=threads[0]["thread_id"])
        return threads[0]["thread_id"]

    logger.info("thread.creating", user_cc=user_companychat)
    return await _create_thread(client, assistant_id, params)


async def _delete_thread(client: LangGraphClient, user_companychat: str) -> None:
    threads = await client.threads.search(
        metadata={"user_companychat": user_companychat},
        sort_by="created_at",
        sort_order="desc",
    )
    for thread in threads:
        await client.threads.delete(thread_id=thread["thread_id"])
    logger.info("thread.old_deleted", count=len(threads))


async def _create_thread(client: LangGraphClient, assistant_id: str, params: AgentRunParams) -> str:
    thread = await client.threads.create(
        graph_id=assistant_id,
        metadata={
            "user_companychat": str(params.user_companychat),

            # init (без None, чтобы не отбрасывалось)
            "last_user_ts": "",
            "last_assistant_ts": "",
            "last_reminder_ts": "",
            "last_dialog_state": "",
            "reminded": 0,

            # реквизиты доставки
            "delivery_user_id": int(params.user_id),
            "delivery_reply_to_history_id": int(params.reply_to_history_id),
            "delivery_access_token": str(params.access_token),
        },
    )

    logger.info("thread.created", thread_id=thread["thread_id"])
    return thread["thread_id"]