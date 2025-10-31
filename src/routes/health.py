"""Модуль реализует endpoint health/ok. Проверка работы langgraph-api."""

from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from ..settings import settings  # type: ignore

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/ok")
async def ok(check_db: int = Query(0, ge=0, le=1)) -> dict[str, Any]:
    """Проверка работы langgraph-api."""
    url = f"{settings.langgraph_url.rstrip('/')}/ok"
    params = {"check_db": check_db}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
