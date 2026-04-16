"""Модуль получения контекстного менеджера langgraph-api."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

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
