# fastapi_app/deps.py
from contextlib import asynccontextmanager
from langgraph_sdk import get_client

from .settings import settings


@asynccontextmanager
async def langgraph_client():
    client = get_client(url=settings.langgraph_url)
    try:
        yield client
    finally:
        await client.aclose()
