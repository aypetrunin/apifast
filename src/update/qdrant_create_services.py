"""Модуль в котором создается в векторной базе Qdrant коллекция по сервисам(типам услуг).

Коллекция создается на основе таблицы services в Postgres.
Порядок действий:
1. Загружает сервисы из Postgres
2. Сбрасывает/создает коллекцию Qdrant
3. Загружает сервисы в коллекцию с эмбеддингами
4. Проверяет работу поиска через retriver_hybrid_async

Примечание: Пересоздается коллекция полностью!!! Нужно переделать частичное по channel_id.
"""

from typing import Any

import asyncpg  # Асинхронный клиент для PostgreSQL
from qdrant_client import models  # Модели и структуры для работы с Qdrant
from tqdm.asyncio import tqdm_asyncio  # Асинхронный прогресс-бар для итераций

from ..settings import settings  # type: ignore
from ..zena_logging import get_logger  # type: ignore

# Импорт общих клиентов и функций из модуля zena_qdrant
from .qdrant_common import (
    ada_embeddings,  # Dense embedding через OpenAI
    batch_iterable,  # Разбивка данных на батчи
    bm25_embedding_model,  # Sparse BM25 embedding
    qdrant_client,  # Асинхронный клиент Qdrant
    reset_collection,  # Сброс/создание коллекции
    retry_request,  # Retry helper для надежной загрузки
)

# Импорт функции для поиска FAQ по гибридной модели
from .qdrant_retriever_faq_services import retriver_hybrid_async

logger = get_logger()

# Название коллекции Qdrant для сервисов
QDRANT_COLLECTION = settings.qdrant_collection_services


# -------------------- Главная асинхронная функция --------------------
async def qdrant_create_services_async(
    collection_name: str = QDRANT_COLLECTION, channel_id: int | None = None, pool: asyncpg.Pool | None = None  # type: ignore[type-arg]
) -> bool:
    """Главная функция для создания коллекции сервисов в Qdrant.

    1. Загружает сервисы из Postgres
    2. Сбрасывает/создает коллекцию Qdrant
    3. Загружает сервисы в коллекцию с эмбеддингами
    4. Проверяет работу поиска через retriver_hybrid_async
    """

    logger.info("qdrant_create_services_async")
    logger.info("Шаг 1: Загрузка данных из Postgres")
    # Шаг 1: Загрузка данных из Postgres
    docs = await services_load_from_postgres(channel_id=channel_id, pool=pool)
    if not docs:
        logger.warning("qdrant.upload.empty", collection=collection_name)
        return False

    logger.info("Шаг 2: Сброс и создание коллекции")
    # Шаг 2: Сброс и создание коллекции
    await reset_collection(qdrant_client, collection_name)

    logger.info("Шаг 3: Загрузка данных в коллекцию")
    # Шаг 3: Загрузка данных в коллекцию
    await fill_collection_services(docs, collection_name)

    logger.info("Шаг 4: Проверка поиска с тестовым запросом")
    # Шаг 4: Проверка поиска с тестовым запросом
    results = await retriver_hybrid_async("Массаж", collection_name, channel_id)
    logger.info("qdrant.search.results", count=len(results))

    # Возвращаем True, если хотя бы один результат найден
    return bool(results)


# -------------------- Загрузка сервисов из Postgres --------------------
async def services_load_from_postgres(
    channel_id: int | None = None,
    pool: asyncpg.Pool | None = None,  # type: ignore[type-arg]
) -> list[dict[str, Any]]:
    """Загружает сервисы из таблицы services.

    Если channel_id указан, фильтрует по нему, иначе возвращает все сервисы.
    Возвращает список словарей с ключами:
    channel_id, id, services_name, description, indications,
    contraindications, pre_session_instructions, body_parts
    """
    assert pool is not None, "pool is required"
    async with pool.acquire() as conn:
        if channel_id is not None:
            rows = await conn.fetch(
                """
                SELECT channel_id, id, services_full_name as services_name, description,
                    indications, contraindications, pre_session_instructions, body_parts
                FROM services
                WHERE channel_id = $1
            """,
                channel_id,
            )
        else:
            rows = await conn.fetch("""
                SELECT channel_id, id, services_full_name as services_name, description,
                    indications, contraindications, pre_session_instructions, body_parts
                FROM services
            """)
        return [dict(r) for r in rows]


# -------------------- Загрузка сервисов в Qdrant --------------------
async def fill_collection_services(
    docs: list[dict[str, Any]], collection_name: str, batch_size: int = 64
) -> None:
    """Загружает сервисы в коллекцию Qdrant.

    Для каждого сервиса создаются два типа эмбеддингов:
        - BM25 (sparse)
        - OpenAI ADA (dense)
    docs: список словарей с сервисами
    collection_name: название коллекции Qdrant
    batch_size: размер батча для пакетной загрузки
    """
    logger.info("qdrant.upload.started", count=len(docs), collection=collection_name)

    # Разбиваем данные на батчи и отображаем прогресс
    for batch in tqdm_asyncio(
        batch_iterable(docs, batch_size), desc="Services batches"
    ):
        # Фильтруем записи без названия сервиса
        filtered = [d for d in batch if d.get("services_name", "").strip()]
        if not filtered:
            continue

        # Получаем список названий для эмбеддинга
        names = [d["services_name"] for d in filtered]

        # -------------------- Эмбеддинги --------------------
        # Sparse BM25 embedding
        bm25_emb = list(bm25_embedding_model.passage_embed(names))
        # Dense OpenAI ADA embedding
        ada_emb = await ada_embeddings(names)

        # -------------------- Формирование точек для Qdrant --------------------
        points = [
            models.PointStruct(
                id=int(d["id"]),  # Используем ID сервиса как идентификатор точки
                vector={
                    "ada-embedding": ada_emb[i],  # Dense вектор
                    "bm25": bm25_emb[i].as_object(),  # Sparse вектор
                },
                payload=d,  # Сохраняем всю запись сервиса как payload
            )
            for i, d in enumerate(filtered)
        ]

        # Загружаем точки в коллекцию с retry для надежности
        await retry_request(
            qdrant_client.upload_points, collection_name=collection_name, points=points
        )


