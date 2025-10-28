"""Модуль вспомогательных функций postgres."""

import asyncio

import asyncpg

from ..settings import settings

POSTGRES_CONFIG = settings.postgres_config


async def is_channel_id(channel_id: int) -> bool:
    """Проверка на наличие channel_id."""
    conn = await asyncpg.connect(**POSTGRES_CONFIG)
    try:
        result = False
        row: asyncpg.Record | None = await conn.fetchrow(
            """
            SELECT cc.id
            FROM channel_chattype cc 
            WHERE cc.channel_id = $1
        """,
            channel_id,
        )
        result = True if row else False

    finally:
        await conn.close()
        return result


if __name__ == "__main__":
    asyncio.run(is_channel_id(11))


# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update_postgres_qdrant.postgres_utils
