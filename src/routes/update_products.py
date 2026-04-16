"""Модуль реализует endpoint update/products."""

import asyncpg
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from ..deps import get_pg_pool
from ..update.postgres_common import is_channel_id  # type: ignore
from ..update.postgres_update_products_services import update_products_services  # type: ignore
from ..update.postgres_update_products import update_products_fields   # type: ignore
from ..update.qdrant_creat_products import qdrant_create_products_async  # type: ignore
from ..zena_logging import get_logger, timed_block

router = APIRouter(prefix="/update", tags=["update"])

logger = get_logger()


@router.post("/products")
async def update_products(channel_id: int, update: bool = False, pool: asyncpg.Pool = Depends(get_pg_pool)) -> JSONResponse:  # type: ignore[type-arg]
    """Определение endpoint."""
    try:
        if not update:
            logger.info("update.products.skipped", channel_id=channel_id, reason="update=False")
            return JSONResponse(
                content={"success": False, "exception": "Параметр: update = False. Для обновления установите: True."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if not await is_channel_id(channel_id, pool):
            logger.info("update.products.not_found", channel_id=channel_id)
            return JSONResponse(
                content={"success": False, "exception": f"Нет фирмы с channel_id = {channel_id}"},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        logger.info("update.products.started", channel_id=channel_id)

        async with timed_block("update.products.postgres_fields"):
            fields_ok = await update_products_fields(channel_id, pool)
        if not fields_ok:
            logger.error("update.products.failed", channel_id=channel_id, stage="postgres_fields")
            return JSONResponse(
                content={"success": False, "exception": f"Ошибка обновления полей в таблице products для channel_id = {channel_id}"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        async with timed_block("update.products.postgres_services"):
            services_ok = await update_products_services(channel_id, pool)
        if not services_ok:
            logger.error("update.products.failed", channel_id=channel_id, stage="postgres_services")
            return JSONResponse(
                content={"success": False, "exception": "Ошибка обновления таблицы products_services - связка products и services."},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        async with timed_block("update.products.qdrant"):
            qdrant_ok = await qdrant_create_products_async(pool)
        if not qdrant_ok:
            logger.error("update.products.failed", channel_id=channel_id, stage="qdrant")
            return JSONResponse(
                content={"success": False, "exception": "Ошибка создания коллекции zena2_products_services_view в qdrant."},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        logger.info("update.products.completed", channel_id=channel_id)
        return JSONResponse(
            content={"success": True, "comment": f"Коллекция 'zena2_products_services_view' пересоздана для channel_id = {channel_id}."},
            status_code=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.exception("update.products.error", channel_id=channel_id, error=str(e))
        return JSONResponse(
            content={"success": False, "exception": f"Ошибка обновления: {e}"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
