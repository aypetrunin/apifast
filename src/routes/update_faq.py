"""Модуль реализует endpoint update/faq."""

import time

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from ..common import logger
from ..update.postgres_common import is_channel_id
from ..update.postgres_update_faq_from_sheet import update_faq_from_sheet
from ..update.qdrant_creat_faq import qdrant_create_faq_async

router = APIRouter(prefix="/update", tags=["update"])


@router.post("/faq")
async def update_faq(channel_id: int, update: bool = False):
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

        if not await update_faq_from_sheet(channel_id):
            msg = f"Ошибка обновления postgres из GoogleSheet для channel_id = {channel_id}"
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        t1 = time.perf_counter()
        if not await qdrant_create_faq_async():
            msg = "Ошибка обновления qdrant из postgres."
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
        msg = f"Данные успешно обновлены из GoogleSheet для channel_id = {channel_id}. "
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
