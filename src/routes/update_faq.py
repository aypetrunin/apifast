"""Модуль реализует endpoint update/faq."""

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from ..update.postgres_common import is_channel_id  # type: ignore
from ..update.postgres_update_faq_from_sheet import (
    update_faq_from_sheet,  # type: ignore
)
from ..update.qdrant_creat_faq import qdrant_create_faq_async  # type: ignore
from ..zena_logging import get_logger, timed_block

router = APIRouter(prefix="/update", tags=["update"])

logger = get_logger()


@router.post("/faq")
async def update_faq(channel_id: int, update: bool = False) -> JSONResponse:
    """Определение endpoint."""
    try:
        if not update:
            logger.info("update.faq.skipped", channel_id=channel_id, reason="update=False")
            return JSONResponse(
                content={"success": False, "exception": "Параметр: update = False. Для обновления установите: True."},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if not await is_channel_id(channel_id):
            logger.info("update.faq.not_found", channel_id=channel_id)
            return JSONResponse(
                content={"success": False, "exception": f"Нет фирмы с channel_id = {channel_id}"},
                status_code=status.HTTP_404_NOT_FOUND,
            )

        logger.info("update.faq.started", channel_id=channel_id)

        async with timed_block("update.faq.postgres"):
            postgres_ok = await update_faq_from_sheet(channel_id)
        if not postgres_ok:
            logger.error("update.faq.failed", channel_id=channel_id, stage="postgres")
            return JSONResponse(
                content={"success": False, "exception": f"Ошибка обновления postgres из GoogleSheet для channel_id = {channel_id}"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        async with timed_block("update.faq.qdrant"):
            qdrant_ok = await qdrant_create_faq_async()
        if not qdrant_ok:
            logger.error("update.faq.failed", channel_id=channel_id, stage="qdrant")
            return JSONResponse(
                content={"success": False, "exception": "Ошибка обновления qdrant из postgres."},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        logger.info("update.faq.completed", channel_id=channel_id)
        return JSONResponse(
            content={"success": True, "comment": f"Данные успешно обновлены из GoogleSheet для channel_id = {channel_id}."},
            status_code=status.HTTP_200_OK,
        )

    except Exception as e:
        logger.exception("update.faq.error", channel_id=channel_id, error=str(e))
        return JSONResponse(
            content={"success": False, "exception": f"Ошибка обновления: {e}"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
