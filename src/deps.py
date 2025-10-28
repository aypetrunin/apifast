"""Модуль получения контекстного менеджера langgraph-api."""

from contextlib import asynccontextmanager

from langgraph_sdk import get_client

from .settings import settings


@asynccontextmanager
async def langgraph_client():
    """Определение контекстного менеджера."""
    client = get_client(url=settings.langgraph_url)
    try:
        yield client
    finally:
        await client.aclose()
