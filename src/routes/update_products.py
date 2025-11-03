"""Модуль реализует endpoint update/products."""

import time

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from ..common import logger  # type: ignore
from ..update.postgres_common import is_channel_id  # type: ignore
from ..update.postgres_update_products_services import update_products_services  # type: ignore
from ..update.postgres_update_products import update_products_fields   # type: ignore
from ..update.qdrant_creat_products import qdrant_create_products_async  # type: ignore

router = APIRouter(prefix="/update", tags=["update"])


@router.post("/products")
async def update_products(channel_id: int, update: bool = False) -> JSONResponse:
    """Определение endpoint."""
    try:
        t0 = time.perf_counter()
        if not update:
            msg = "Параметр: update = False. Для обновления установите: True."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if not await is_channel_id(channel_id):
            msg = f"Нет фирмы с channel_id = {channel_id}"
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        if not await update_products_fields(channel_id):
            msg = f"Ошибка обновления полей в таблице products для channel_id = {channel_id}"
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        if not await update_products_services(channel_id):
            msg = "Ошибка обновления таблицы products_services - связка products и services."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        t1 = time.perf_counter()
        if not await qdrant_create_products_async():
            msg = "Ошибка создания коллекции zena2_products_services_view в qdrant."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        t2 = time.perf_counter()
        t_all = t2 - t0
        t_postgres = t1 - t0
        t_qdrant = t2 - t1
        msg_time = f"Общее время: {t_all:.2f} сек., Postgres: {t_postgres:.2f} сек., Qdrant: {t_qdrant:.2f} сек."
        msg = f"Коллекция 'zena2_products_services_view' пересоздана для channel_id = {channel_id}. "
        logger.info(msg)
        return JSONResponse(
            content={"success": True, "comment": msg, "time": msg_time},
            status_code=status.HTTP_200_OK,
        )

    except Exception as e:
        msg = f"Ошибка обновления: {e}"
        logger.exception(msg)
        return JSONResponse(
            content={"success": False, "exception": msg},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
