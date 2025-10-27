"""Модуль обновления двух полей product_full_name, product_unid_ean на основе других полей.
Запускается после обновления таблицы products из CRM.
 """

import asyncio
import asyncpg

from ..common import logger
from ..settings import settings
from .postgres_products_utils import classify, sanitize_name


async def update_products(channel_id: int):
    """Функция обновления полей product_full_name, product_unid_ean.
    """
    conn = await asyncpg.connect(**settings.postgres_config)
    try:
        if channel_id == 1:
            result = await _update_products_channel1(conn, channel_id)
        elif channel_id in [2, 5, 6]:
            result = await _update_products_channel2(conn, channel_id)
        else:
            result = f"❌ Не поддерживается channel_id={channel_id}"
        logger.info(f"✅ Обновлено записей для channel_id={channel_id}: {result}")
        return result
    finally:
        await conn.close()


async def _update_products_channel1(conn, channel_id: int):
    """Обновление продуктов для София (channel_id=1).
    """
    result = await conn.execute(
        """
        UPDATE products
        SET
            product_full_name = service_value || '. ' || product_name,
            product_unid_ean = service_value
        WHERE channel_id = $1
    """,
        channel_id,
    )
    return result


async def _update_products_channel2(conn, channel_id: int):
    """Обновление продуктов для Алиса (channel_id=2)
    """
    rows = await conn.fetch(
        """
        SELECT product_id, product_name, service_value, description
        FROM products WHERE channel_id=$1
    """,
        channel_id,
    )
    update_data = []
    for row in rows:
        product_unid_ean = classify(
            row["product_name"],
            row["service_value"],
            row["description"] or "",
            debug=False,
        )
        product_full_name = f"{product_unid_ean} - {sanitize_name(row['product_name'])}"
        update_data.append((product_unid_ean, product_full_name, row["product_id"]))

    if update_data:
        await conn.executemany(
            """
            UPDATE products SET product_unid_ean=$1, product_full_name=$2 WHERE product_id=$3
        """,
            update_data,
        )
    return f"UPDATE {len(update_data)}"


if __name__ == "__main__":
    """Тест запуска функции обновления FAQ для канала с id=1."""
    asyncio.run(update_products(1))

# Запуск для проверки
# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update_postgres_qdrant.update_products
