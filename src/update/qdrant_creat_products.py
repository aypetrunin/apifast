"""Модуль реализует процесс создания коллекции для поиска услуг/продуктов."""

import asyncio
from typing import Any

import asyncpg  # Асинхронный клиент для PostgreSQL
from qdrant_client import models  # Модели для работы с Qdrant
from tqdm.asyncio import tqdm_asyncio  # Асинхронный прогресс-бар

from ..common import logger  # type: ignore
from ..settings import settings  # type: ignore
from .qdrant_common import (
    ada_embeddings,  # Dense embeddings через OpenAI
    batch_iterable,  # Генератор для разбивки на батчи
    bm25_embedding_model,  # BM25 sparse embedding
    qdrant_client,  # Асинхронный клиент Qdrant
    reset_collection,  # Функция сброса/создания коллекции
    retry_request,  # Retry helper для надёжного выполнения
)
from .qdrant_retriever_product import retriever_product_hybrid_async

# Название коллекции Qdrant для продуктов и услуг
QDRANT_COLLECTION = settings.qdrant_collection_products
POSTGRES_CONFIG = settings.postgres_config

# Поля для создания текстовых индексов в Qdrant
TEXT_INDEX_FIELDS = [
    "indications_key",
    "contraindications_key",
    "body_parts",
    "product_type",
]


# -------------------- Главная асинхронная функция --------------------
async def qdrant_create_products_async() -> bool:
    """Главная функция для создания коллекции продуктов в Qdrant.

    1. Загружает продукты из Postgres
    2. Сбрасывает/создаёт коллекцию Qdrant с текстовыми индексами
    3. Загружает продукты в коллекцию с эмбеддингами
    4. Проверяет работу поиска через retriver_product_hybrid_async
    """
    # Шаг 1: Загрузка данных
    docs = await products_load_from_postgres()
    if not docs:
        logger.warning("Нет данных для загрузки.")
        return False

    # Шаг 2: Сброс и создание коллекции с текстовыми индексами
    await reset_collection(
        qdrant_client, QDRANT_COLLECTION, text_index_fields=TEXT_INDEX_FIELDS
    )

    # Шаг 3: Загрузка данных в коллекцию
    await fill_collection_products(docs, QDRANT_COLLECTION)

    # Шаг 4: Проверка поиска (пример запроса)
    results = await retriever_product_hybrid_async(1, "массаж")
    logger.info(f"Найдено результатов: {len(results)}")

    # Возвращаем True, если хотя бы один результат найден
    return bool(results)


# -------------------- Загрузка продуктов из Postgres --------------------
async def products_load_from_postgres() -> list[dict[str, Any]]:
    """Загружает все продукты и услуги из представления product_service_view в Postgres.

    Возвращает список словарей, где каждая запись содержит все колонки из представления.
    """
    conn = await asyncpg.connect(**POSTGRES_CONFIG)  # Подключение к БД
    try:
        rows = await conn.fetch("""SELECT * FROM product_service_view""")
        return [dict(r) for r in rows]  # Преобразуем результат в список словарей
    finally:
        await conn.close()  # Закрываем соединение


# -------------------- Загрузка продуктов в Qdrant --------------------
async def fill_collection_products(
    docs: list[dict[str, Any]], collection_name: str, batch_size: int = 64
) -> None:
    """Загружает данные о продуктах в коллекцию Qdrant.

    Для каждой записи создаются два вида эмбеддингов:
        - BM25 (sparse)
        - OpenAI ADA (dense)
    docs: список словарей с продуктами
    collection_name: название коллекции Qdrant
    batch_size: размер батча для пакетной загрузки
    """
    logger.info(f"Загрузка {len(docs)} продуктов в '{collection_name}'")

    # Разбиваем данные на батчи и отображаем прогресс
    for batch in tqdm_asyncio(
        batch_iterable(docs, batch_size), desc="Products batches"
    ):
        # Фильтруем записи без текста для поиска
        filtered = [d for d in batch if d.get("product_search", "").strip()]
        if not filtered:
            continue

        # Получаем список текстов для эмбеддинга
        searches = [d["product_search"] for d in filtered]

        # -------------------- Эмбеддинги --------------------
        # Sparse BM25 embeddings
        bm25_emb = list(bm25_embedding_model.passage_embed(searches))
        # Dense OpenAI embeddings
        ada_emb = await ada_embeddings(searches)

        # -------------------- Формирование точек Qdrant --------------------
        points = [
            models.PointStruct(
                id=int(d["id"]),  # ID точки соответствует ID продукта
                vector={"ada-embedding": ada_emb[i], "bm25": bm25_emb[i].as_object()},
                payload=d,  # Полная запись сохраняется в payload
            )
            for i, d in enumerate(filtered)
        ]

        # Загружаем точки в коллекцию с retry для надёжности
        await retry_request(
            qdrant_client.upload_points, collection_name=collection_name, points=points
        )


# -------------------- Запуск скрипта --------------------
if __name__ == "__main__":
    # Асинхронный запуск основной функции
    asyncio.run(qdrant_create_products_async())


# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update.qdrant_creat_products
