"""Модуль получения контекстного менеджера langgraph-api."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from fastapi import Request
from langgraph_sdk import get_client
from langgraph_sdk.client import LangGraphClient

from .settings import settings
from .zena_logging import get_logger

logger = get_logger()

@asynccontextmanager
async def langgraph_client() -> AsyncGenerator[LangGraphClient, None]:
    """Определение контекстного менеджера."""
    client = get_client(url=settings.langgraph_url)
    try:
        yield client
    finally:
        await client.aclose()


def get_pg_pool(request: Request) -> asyncpg.Pool:  # type: ignore[type-arg]
    """Достаёт пул из app.state."""
    return request.app.state.pg_pool
