"""Модуль создания endpointa '/agent/run' - агента-бота."""

import time

from typing_extensions import Any
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from langgraph_sdk.client import LangGraphClient
from langgraph_sdk.schema import Assistant

from ..common import logger  # type: ignore
from ..deps import langgraph_client  # type: ignore
from ..requests.httpservice import sent_message_to_history  # type: ignore
from ..schemas import AgentRunParams  # type: ignore


router = APIRouter(prefix="/agent", tags=["agent"])

@router.post("/run")
async def run_sync(params: AgentRunParams) -> JSONResponse:
    """Определение endpoint."""
    info = "--NOT--"
    try:
        logger.info("===router/run===")
        logger.info(f"params: {params}")

        t0=t1=t2 = time.perf_counter()
        async with langgraph_client() as client:
            logger.info("===langgraph_client===")
            assistant_id = await get_or_create_assistant(client, params)
            thread_id = await get_or_create_thread(client, assistant_id, params)

            logger.info(f"assistant_id: {assistant_id}")
            logger.info(f"thread_id: {thread_id}")

            run = await client.runs.create(
                thread_id=thread_id,
                assistant_id=assistant_id,
                input={"messages": [{"role": "user", "content": params.message}]},
                config=params.config,
                context=params.context,
                metadata=params.metadata,
                on_completion="delete",
                # durability= True,
            )

            t1 = time.perf_counter()

            agent_responce = await client.runs.join(
                thread_id=run["thread_id"],
                run_id=run["run_id"],
            )
            t2 = time.perf_counter()

        msgs = agent_responce.get("messages")
        logger.info(f"msgs: {agent_responce}")
        # text = msgs[-1]["content"] if isinstance(msgs, list) and msgs else "Ошибка ...."
        text = _content_to_text(msgs[-1]["content"])

        info = "--OK--"
        success_response = {"success": True, "exception": "no", "message": text}
        status_code = status.HTTP_200_OK

    except Exception as e:
        success_response = {"success": False, "exception": str(e)}
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    logger.info(f"\n--HUMAN--: {params.message}")
    logger.info(f"-----AI--: {text}\n") 

    assistant = f'{params.assistant_id}/{params.user_companychat}'
    if info == "--OK--":
        logger.info(f"{info}: agent({assistant}) - create:{t1 - t0:.3f}s, exec:{t2 - t1:.3f}s, all:{t2 - t0:.3f}s")
    else:
        logger.error(f"{info}. {success_response['exception']}")

    return JSONResponse(content=success_response, status_code=status_code)


async def get_or_create_assistant(
        client: LangGraphClient,
        params: AgentRunParams
) -> str:
    """Получить/создать ассистента для пользователя"""
    
    logger.info("===get_or_create_user_assistant===")

    user_companychat = str(params.user_companychat)

    assistants: list[Assistant] = await client.assistants.search(
        metadata={"user_companychat": user_companychat}
    )

    if assistants:
        logger.info("✅ Assistant exists!")
        return assistants[0]["assistant_id"]
    
    assistant: Assistant = await client.assistants.create(
        name=f"user_{user_companychat}",
        context=params.context,
        graph_id=params.assistant_id,
        metadata={"user_companychat": user_companychat},
    )
    logger.info("✅ Assistant created successfully!")
    return assistant["assistant_id"]


async def get_or_create_thread(
        client: LangGraphClient,
        assistant_id: str,
        params: AgentRunParams,

) -> str:
    """Получить/создать поток ассистента для пользователя"""
    
    logger.info("===get_or_create_thread===")
    
    last_message = params.message.strip().lower()
    user_companychat = str(params.user_companychat)

    if last_message == 'стоп':
        logger.info(f"🛑 Стоп-слово для user_companychat={user_companychat}")
        await _delete_thread(client, user_companychat)
        return await _create_thread(client, assistant_id, params)

    threads = await client.threads.search(
        metadata={"user_companychat": user_companychat},
        sort_by="created_at",
        sort_order="desc",
        limit=1
    )

    if threads:
        logger.info(f"✅ Поток найден: {threads[0]['thread_id']}")
        return threads[0]["thread_id"]

    logger.info(f"📝 Создаем первый поток для user_companychat={user_companychat}")
    return await _create_thread(client, assistant_id, params)


async def _delete_thread(client: LangGraphClient, user_companychat: str):
        """Удаление старых потоков. Страховочное удаление."""
        
        threads = await client.threads.search(
            metadata={"user_companychat": user_companychat},
            sort_by="created_at", sort_order="desc",
        )
        for thread in threads:
            await client.threads.delete(thread_id=thread['thread_id'])
        logger.info("✅ Старые потоки удалены!")


async def _create_thread(
    client: LangGraphClient,
    assistant_id: str,
    params: AgentRunParams
) -> str:
    """Функция создания потока."""
    
    ttl = 20 if params.mcp_port in [5020] else 1440
    thread = await client.threads.create(
        graph_id=assistant_id,
        metadata={"user_companychat": str(params.user_companychat)},
        ttl={"ttl": ttl, "strategy": "delete"}
    )
    logger.info("✅ Новый поток создан!")
    return thread["thread_id"]


def _content_to_text(content: str | list[Any] | None) -> str:
    """Функция получения сообщения.

    Функция возвращает content из HumanMessages в зависимости от того
    где оно было сформировано Langgraph Studio в закладке Chat или Graph.
    Особенность Langgraph Studio.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list) and content:
        part = content[0]
        if isinstance(part, dict):
            if "text" in part and isinstance(part["text"], str):
                return part["text"]
            if "content" in part and isinstance(part["content"], str):
                return part["content"]
    return ""




# @router.post("/run")
# async def run_stream(params: AgentRunParams):
#     messages, context = build_messages_and_context(params)

#     async def stream():
#         async with langgraph_client() as client:
#             async for part in client.runs.stream(
#                 thread_id=None,
#                 assistant_id=params.assistant_id, 
#                 input={"messages": messages},
#                 stream_mode=["values", "debug"],
#                 config=params.config,
#                 context=context,
#                 metadata=params.metadata,
#                 on_completion="delete",
#             ):
#                 if isinstance(part, Mapping):
#                     payload = json.dumps(part, ensure_ascii=False, default=str)
#                 else:
#                     try:
#                         payload = part.json()
#                     except AttributeError:
#                         try:
#                             payload = json.dumps(
#                                 part.__dict__, ensure_ascii=False, default=str
#                             )
#                         except Exception:
#                             payload = json.dumps(
#                                 {"event": str(part)}, ensure_ascii=False
#                             )
#                 yield f"data: {payload}\n\n".encode("utf-8")

#     return StreamingResponse(stream(), media_type="text/event-stream")
