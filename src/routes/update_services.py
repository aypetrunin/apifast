"""Модуль реализует endpoint update/services."""

import asyncpg
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse

from ..deps import get_pg_pool
from ..update.postgres_common import is_channel_id  # type: ignore
from ..update.postgres_update_products_services import (
    update_products_services,  # type: ignore
)
from ..update.postgres_update_services_from_sheet import (
    update_services_from_sheet,  # type: ignore
)
from ..update.qdrant_creat_products import qdrant_create_products_async  # type: ignore
from ..update.qdrant_create_services import qdrant_create_services_async  # type: ignore
from ..zena_logging import get_logger, timed_block

router = APIRouter(prefix="/update", tags=["update"])

logger = get_logger()


@router.post("/services")
async def update_services(channel_id: int, update: bool = False, pool: asyncpg.Pool = Depends(get_pg_pool)) -> JSONResponse:  # type: ignore[type-arg]
    """Определение endpoint."""
    try:
        if not update:
            logger.info("update.services.skipped", channel_id=channel_id, reason="update=False")
            return JSONResponse(
                content={"success": False, "exception": "Параметр: update = False. Для обновления установите: True."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if not await is_channel_id(channel_id, pool):
            logger.info("update.services.not_found", channel_id=channel_id)
            return JSONResponse(
                content={"success": False, "exception": f"Нет фирмы с channel_id = {channel_id}"},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        logger.info("update.services.started", channel_id=channel_id)

        async with timed_block("update.services.postgres"):
            postgres_ok = await update_services_from_sheet(channel_id, pool)
        if not postgres_ok:
            logger.error("update.services.failed", channel_id=channel_id, stage="postgres")
            return JSONResponse(
                content={"success": False, "exception": f"Ошибка обновления postgres из GoogleSheet для channel_id = {channel_id}"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        async with timed_block("update.services.qdrant_services"):
            qdrant_services_ok = await qdrant_create_services_async(pool=pool)
        if not qdrant_services_ok:
            logger.error("update.services.failed", channel_id=channel_id, stage="qdrant_services")
            return JSONResponse(
                content={"success": False, "exception": "Ошибка обновления коллекции services в qdrant из postgres."},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        async with timed_block("update.services.postgres_products_services"):
            products_services_ok = await update_products_services(channel_id, pool)
        if not products_services_ok:
            logger.error("update.services.failed", channel_id=channel_id, stage="postgres_products_services")
            return JSONResponse(
                content={"success": False, "exception": "Ошибка обновления таблицы products_services - связка products и services."},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        async with timed_block("update.services.qdrant_products"):
            qdrant_products_ok = await qdrant_create_products_async(pool)
        if not qdrant_products_ok:
            logger.error("update.services.failed", channel_id=channel_id, stage="qdrant_products")
            return JSONResponse(
                content={"success": False, "exception": "Ошибка создания коллекции zena2_products_services_view в qdrant."},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        logger.info("update.services.completed", channel_id=channel_id)
        return JSONResponse(
            content={"success": True, "comment": f"Данные успешно обновлены из GoogleSheet для channel_id = {channel_id}."},
            status_code=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.exception("update.services.error", channel_id=channel_id, error=str(e))
        return JSONResponse(
            content={"success": False, "exception": f"Ошибка обновления: {e}"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
