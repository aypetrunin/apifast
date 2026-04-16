"""Модуль обновления двух полей product_full_name, product_unid_ean на основе других полей.

Запускается после обновления таблицы products из CRM.
"""

import asyncpg
from asyncpg import Connection

from ..zena_logging import get_logger  # type: ignore
from .postgres_products_utils import classify, sanitize_name

logger = get_logger()


async def update_products_fields(channel_id: int, pool: asyncpg.Pool) -> bool:  # type: ignore[type-arg]
    """Функция обновления полей product_full_name, product_unid_ean."""
    async with pool.acquire() as conn:
        result = ''
        if channel_id in [2]:
            result = await _update_products_channel2(conn, channel_id)
        else:
            result = await _update_products_channel1(conn, channel_id)

        logger.info(
            "update.products.completed",
            channel_id=channel_id,
            result=result,
        )

        return bool(result)


async def _update_products_channel1(conn: Connection, channel_id: int) -> str:
    """Обновление продуктов для София (channel_id=1)."""
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


async def _update_products_channel2(conn: Connection, channel_id: int) -> str:
    """Обновление продуктов для Алиса (channel_id=2)."""
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


