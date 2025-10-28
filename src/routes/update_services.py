import time

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from ..common import logger
from ..update.postgres_common import is_channel_id
from ..update.qdrant_create_services import qdrant_create_services_async
from ..update.postgres_update_services_from_sheet import update_services_from_sheet
from ..update.postgres_update_products_services import update_products_services
from ..update.qdrant_creat_products import qdrant_create_products_async

router = APIRouter(prefix="/update", tags=["update"])

@router.post("/services")
async def update_services(channel_id: int, update: bool = False):
    try:
        t0 = time.perf_counter()
        if not update:
            msg = "Параметр: update = False. Для обновления установите: True."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_400_BAD_REQUEST
            )

        if not await is_channel_id(channel_id):
            msg = f"Нет фирмы с channel_id = {channel_id}"
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_404_NOT_FOUND
            )

        if not await update_services_from_sheet(channel_id):
            msg = f"Ошибка обновления postgres из GoogleSheet для channel_id = {channel_id}"
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        t1 = time.perf_counter()
        if not await qdrant_create_services_async():
            msg = "Ошибка обновления коллекции services в qdrant из postgres."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        t2 = time.perf_counter()
        if not await update_products_services(channel_id):
            msg = "Ошибка обновления таблицы products_services - связка products и services."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        t3 = time.perf_counter()
        if not await qdrant_create_products_async():
            msg = "Ошибка создания коллекции zena2_products_services_view в qdrant."
            logger.info(msg)
            return JSONResponse(
                content={"success": False, "exception": msg},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        t4 = time.perf_counter()
        t_all = t4 - t0
        # t_services = t1 - t0
        # t_qdrant = t2-t1
        # t_product_services = t3-t2
        msg_time = f"Общее время: {t_all:.2f} сек."
        msg = f"Данные успешно обновлены из GoogleSheet для channel_id = {channel_id}. "
        logger.info(msg)
        return JSONResponse(
            content={"success": True, "comment": msg, "time": msg_time},
            status_code=status.HTTP_200_OK
        )

    except Exception as e:
        msg = f"Ошибка обновления: {e}"
        logger.exception(msg)
        return JSONResponse(
            content={"success": False, "exception": msg},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
