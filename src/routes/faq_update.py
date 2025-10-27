
from fastapi import APIRouter, HTTPException, Query

from ..settings import settings
from ..common import logger
from ..update_postgres_qdrant.postgres_common import is_channel_id
from ..update_postgres_qdrant.postgres_update_faq_from_sheet import update_faq_from_sheet
from ..update_postgres_qdrant.qdrant_creat_faq import qdrant_create_faq_async

router = APIRouter(prefix="/faq_update", tags=["faq_update"])

@router.post("/faq_update")
async def faq_update(channel_id: int, update: bool = False):

    try:
        if not update:
            msg = f"Параметр: update = False. Для обновления установите: True"
            logger.info(msg)
            return {
                "success": False,
                "comment": msg,
            }
        if not await is_channel_id(channel_id):
            msg = f"Нет фирмы с channel_id = {channel_id}"
            logger.info(msg)
            return {
                "success": False,
                "comment": msg,
            }
        if not await update_faq_from_sheet(channel_id):
            msg = f"Ошибка обновления postres из GoogleSheet для channel_id = {channel_id}"
            logger.info(msg)
            return {
                "success": False,
                "comment": msg,
            }
        if not await qdrant_create_faq_async():
            msg = f"Ошибка обновления qdrant из postgres."
            logger.info(msg)
            return {
                "success": False,
                "comment": msg,
            }
        return {
                "success": True,
                "comment": f"Данные успешно обновлены из GoogleSheet для channel_id = {channel_id}",
            }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
