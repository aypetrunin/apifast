"""Модуль вспомогательных функций postgres."""

import asyncpg


async def is_channel_id(channel_id: int, pool: asyncpg.Pool) -> bool:  # type: ignore[type-arg]
    """Проверка на наличие channel_id."""
    async with pool.acquire() as conn:
        row: asyncpg.Record | None = await conn.fetchrow(
            """
            SELECT cc.id
            FROM channel_chattype cc
            WHERE cc.channel_id = $1
        """,
            channel_id,
        )
        return bool(row)


