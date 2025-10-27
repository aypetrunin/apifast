import json
import time

from collections.abc import Mapping
from typing import Tuple, List, Dict, Any, Optional

from fastapi import APIRouter, status
from fastapi.responses import StreamingResponse, JSONResponse

from ..deps import langgraph_client
from ..schemas import AgentRunParams
from ..common import logger
from ..requests.httpservice import sent_message_to_history


router = APIRouter(prefix="/agent", tags=["agent"])

class PayloadError(ValueError):
    pass

def build_messages_and_context(
    params: "AgentRunParams",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    # базовая семантическая проверка
    if not params.message or not isinstance(params.message, str):
        raise PayloadError("message must be a non-empty string")
    if not params.access_token:
        raise PayloadError("access_token is required")

    messages = [{"role": "user", "content": params.message}]
    context = {
        "_user_companychat": params.user_companychat,
        "_reply_to_history_id": params.reply_to_history_id,
        "_access_token": params.access_token,
        "_user_id": params.user_id,
        "_group_id": params.group_id,
        "_platform": params.platform,
    }
    return messages, context


@router.post("/run_sync")
async def run_sync(params: AgentRunParams):
    t0 = time.perf_counter()
    messages: Optional[list[dict[str, Any]]] = None
    context: Optional[dict[str, Any]] = None
    final_state: Optional[dict[str, Any]] = None
    t_build = t1 = t2 = None
    payload: Optional[dict[str, Any]] = None
    info = "--NOT--"
    status_code = status.HTTP_200_OK
    success_response: dict[str, Any] = {"success": False, "exception": "uninitialized"}

    try:
        messages, context = build_messages_and_context(params)
        t_build = time.perf_counter()

        async with langgraph_client() as client:
            run = await client.runs.create(
                thread_id=None,
                assistant_id=params.assistant_id,
                input={"messages": messages},
                config=params.config,
                context=context,
                metadata=params.metadata,
                on_completion="delete",
            )
            t1 = time.perf_counter()

            final_state = await client.runs.join(
                thread_id=run["thread_id"],
                run_id=run["run_id"],
            )
            t2 = time.perf_counter()

        msgs = final_state.get("messages")
        text = msgs[-1]["content"] if isinstance(msgs, list) and msgs else ""
        payload = {
            "user_id": context.get("_user_id"),
            "text": text,
            "user_companychat": context.get("_user_companychat"),
            "reply_to_history_id": context.get("_reply_to_history_id"),
            "access_token": context.get("_access_token"),
            "tokens": final_state.get("tokens"),
            "tools": final_state.get("tools_name", []),
            "tools_args": final_state.get("tools_args", {}),
            "tools_result": final_state.get("tools_results", {}),
            "prompt_system": final_state.get("prompt_system", ""),
            "template_prompt_system": final_state.get("template_prompt_system", ""),
            "dialog_state": final_state.get("dialog_state", ""),
            "dialog_state_new": final_state.get("dialog_state_new", ""),
        }

        info = "--OK--"
        success_response = {"success": True, "exception": "no", "message": text}
        status_code = status.HTTP_200_OK

    except PayloadError as e:
        payload = {"text": "Бот временно не работает"}
        success_response = {"success": False, "exception": str(e)}
        status_code = status.HTTP_422_UNPROCESSABLE_ENTITY

    except Exception as e:
        payload = {"text": "Бот временно не работает"}
        success_response = {"success": False, "exception": str(e)}
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

    finally:
        t3 = time.perf_counter()
        try:
            t_save0 = time.perf_counter()
            logger.info(f"--AI--: {payload.get('text')[:50]} .....")
            await sent_message_to_history(**payload)

            t_save1 = time.perf_counter()
            tok = (context or {}).get("_access_token")
            tok_mask = f"{tok[:5]}" if tok else "NA"

            d_build = (t_build - t0) if t_build is not None else 0.0
            d_create = (
                (t1 - t_build) if (t1 is not None and t_build is not None) else 0.0
            )
            d_exec = (t2 - t1) if (t2 is not None and t1 is not None) else 0.0
            d_save = (
                t_save1 - t_save0
            )
            d_all = t3 - t0

            if info == "--OK--":
                logger.info(
                    f"{info}: agent({tok_mask}) - build:{d_build:.3f}s, create:{d_create:.3f}s, exec:{d_exec:.3f}s, save:{d_save:.3f}s, all:{d_all:.3f}s"
                )
            else:
                logger.error(f"{info}. {success_response['exception']}")

            return JSONResponse(content=success_response, status_code=status_code)

        except Exception as e2:
            logger.error(f"save_to_postgres_error: {e2}")


@router.post("/run")
async def run_stream(params: AgentRunParams):
    messages, context = build_messages_and_context(params)

    async def stream():
        async with langgraph_client() as client:
            async for part in client.runs.stream(
                thread_id=None,
                assistant_id=params.assistant_id,
                input={"messages": messages},
                stream_mode=["values", "debug"],
                config=params.config,
                context=context,
                metadata=params.metadata,
                on_completion="delete",
            ):
                if isinstance(part, Mapping):
                    payload = json.dumps(part, ensure_ascii=False, default=str)
                else:
                    try:
                        payload = part.json()
                    except AttributeError:
                        try:
                            payload = json.dumps(
                                part.__dict__, ensure_ascii=False, default=str
                            )
                        except Exception:
                            payload = json.dumps(
                                {"event": str(part)}, ensure_ascii=False
                            )
                yield f"data: {payload}\n\n".encode("utf-8")

    return StreamingResponse(stream(), media_type="text/event-stream")
