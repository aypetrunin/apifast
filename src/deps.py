"""Модуль получения контекстного менеджера langgraph-api."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from langgraph_sdk import get_client
from langgraph_sdk.client import LangGraphClient

from .settings import settings


@asynccontextmanager
async def langgraph_client() -> AsyncGenerator[LangGraphClient, None]:
    """Определение контекстного менеджера."""
    client = get_client(url=settings.langgraph_url)
    try:
        yield client
    finally:
        await client.aclose()
