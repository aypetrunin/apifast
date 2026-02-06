import logging

from datetime import datetime, timezone, timedelta
from typing import Any

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse


from ..deps import langgraph_client  # type: ignore
from ..requests.httpservice import sent_message_to_history  # type: ignore

from .agent import _patch_thread_metadata, _utc_iso, _content_to_text

logger = logging.getLogger(__name__)

reminders_router = APIRouter(prefix="/agent/reminders", tags=["reminders"])


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _extract_state_messages(thread_state: Any) -> list[Any]:
    if not isinstance(thread_state, dict):
        return []

    state = thread_state.get("values") or thread_state.get("state") or thread_state
    if not isinstance(state, dict):
        return []

    msgs = state.get("messages") or []
    return msgs if isinstance(msgs, list) else []


def _extract_reminder_text(agent_resp: Any) -> str:
    if not isinstance(agent_resp, dict):
        return ""

    msgs = agent_resp.get("messages")
    if not isinstance(msgs, list) or not msgs:
        return ""

    last_msg = msgs[-1]
    # Частый формат: dict с полем content
    if isinstance(last_msg, dict) and "content" in last_msg:
        return _content_to_text(last_msg["content"]).strip()

    # fallback
    return _content_to_text(last_msg).strip()


@reminders_router.post("/check")
async def reminders_check(body: dict[str, Any] | None = None) -> JSONResponse:
    """
    POST /agent/reminders/check
    body (опционально):
      {
        "timeout_minutes": 5,         # ждать после ответа ассистента
        "cooldown_minutes": 5,        # НЕ чаще чем раз в N минут (если нужно 12 часов — ставьте 720)
        "reminder_limit": 2,          # сколько раз делать восстановление диалога
      }
    """

    logger.info("router - reminders_check")

    body = body or {}
    timeout_minutes = _safe_int(body.get("timeout_minutes", 5), 5)
    cooldown_minutes = _safe_int(body.get("cooldown_minutes", 5), 5)
    reminder_limit = _safe_int(body.get("reminder_limit", 2), 2)

    now = datetime.now(timezone.utc)

    scanned = 0
    reminded_total = 0
    skipped_no_delivery = 0

    async with langgraph_client() as client:
        try:
            threads = await client.threads.search(
                sort_by="created_at",
                sort_order="desc",
            )


        except TypeError:
            return JSONResponse(
                content={"success": False, "error": "Ошибка чтения threads"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        for th in threads:
            scanned += 1
            thread_id = th.get("thread_id")
            md = th.get("metadata") or {}

            user_companychat = md.get("user_companychat")
            last_user_ts = _parse_iso(md.get("last_user_ts"))
            last_assistant_ts = _parse_iso(md.get("last_assistant_ts"))
            last_reminder_ts = _parse_iso(md.get("last_reminder_ts"))
            last_dialog_state = md.get("last_dialog_state", "new")
            reminded = _safe_int(md.get("reminded", 0), 0)

            if not thread_id or not user_companychat:
                continue

            if last_dialog_state in ("new", None):
                continue
            
            if reminded >= reminder_limit:
                continue

            # 1) должно быть что напоминать
            if not last_assistant_ts:
                continue

            # 2) ассистент должен быть последним говорящим
            if last_user_ts and last_user_ts > last_assistant_ts:
                continue

            # 3) таймаут
            if now - last_assistant_ts < timedelta(minutes=timeout_minutes):
                continue

            # 4) cooldown
            if last_reminder_ts and now - last_reminder_ts < timedelta(minutes=cooldown_minutes):
                continue

            # 5) реквизиты доставки
            delivery = {
                "delivery_user_id": md.get("delivery_user_id"),
                "delivery_reply_to_history_id": md.get("delivery_reply_to_history_id", 0),
                "delivery_access_token": md.get("delivery_access_token", ""),
            }
            if not delivery["delivery_user_id"] or not delivery["delivery_access_token"]:
                skipped_no_delivery += 1
                continue

            # 6) достаём messages только теперь (дорогой вызов)
            try:
                thread_state = await client.threads.get_state(thread_id)
            except Exception as e:
                logger.warning("get_state failed ucc=%s thread=%s: %s", user_companychat, thread_id, e)
                continue

            messages = _extract_state_messages(thread_state)
            if not messages:
                continue

            # 7) генерим напоминание
            try:
                agent_redialog_response = await client.runs.wait(
                    None,
                    assistant_id="agent_zena_redialog",
                    input={"messages": messages},
                    on_completion="delete",
                )
            except Exception as e:
                logger.exception("agent_redialog failed ucc=%s thread=%s: %s", user_companychat, thread_id, e)
                continue

            reminder_text = _extract_reminder_text(agent_redialog_response)
            if not reminder_text:
                logger.warning("empty reminder_text ucc=%s thread=%s", user_companychat, thread_id)
                continue

            logger.info("agent_redialog_response: %s", reminder_text)

            # 8) отправка напоминания
            try:
                await sent_message_to_history(
                    user_id=int(delivery["delivery_user_id"]),
                    text=reminder_text,
                    user_companychat=int(user_companychat),
                    reply_to_history_id=int(delivery["delivery_reply_to_history_id"] or 0),
                    access_token=str(delivery["delivery_access_token"]),
                    tokens={},
                    tools=[],
                    tools_args={},
                    tools_result={},
                    prompt_system="",
                    template_prompt_system="",
                    dialog_state="",
                    dialog_state_new="",
                )
            except Exception as e:
                logger.exception("Reminder send failed ucc=%s thread=%s: %s", user_companychat, thread_id, e)
                continue

            reminded += 1
            reminded_total += 1

            # 9) фиксируем last_reminder_ts + reminded (и сохраняем delivery)
            try:
                await _patch_thread_metadata(
                    client,
                    thread_id,
                    {
                        "user_companychat": str(user_companychat),
                        "last_reminder_ts": _utc_iso(),
                        "reminded": reminded,
                        **delivery,
                    },
                )
            except Exception as e:
                logger.warning(
                    "⚠️ metadata patch failed ucc=%s thread=%s: %s",
                    user_companychat,
                    thread_id,
                    e,
                )

    return JSONResponse(
        content={
            "success": True,
            "timeout_minutes": timeout_minutes,
            "cooldown_minutes": cooldown_minutes,
            "reminder_limit": reminder_limit,
            "scanned": scanned,
            "reminded_total": reminded_total,  # FIX
            "skipped_no_delivery": skipped_no_delivery,
        },
        status_code=status.HTTP_200_OK,
    )
