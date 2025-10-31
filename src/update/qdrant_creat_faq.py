"""Модуль в котором создается в векторной базе Qdrant коллекция по FAQ.

Коллекция создается на основе таблицы faq в Postgres.
Порядок действий:
1. Загружает сервисы из Postgres
2. Сбрасывает/создает коллекцию Qdrant
3. Загружает сервисы в коллекцию с эмбеддингами
4. Проверяет работу поиска через retriver_hybrid_async

Примечание: Пересоздается коллекция полностью!!! Нужно переделать частичное по channel_id.
"""

import asyncio
from typing import Any

import asyncpg  # Асинхронный клиент для PostgreSQL
from qdrant_client import models  # Модели для работы с точками Qdrant
from tqdm.asyncio import tqdm_asyncio  # Асинхронный прогресс-бар для итераций

from ..common import logger  # type: ignore
from ..settings import settings  # type: ignore

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

# Название коллекции в Qdrant
QDRANT_COLLECTION = settings.qdrant_collection_faq
POSTGRES_CONFIG = settings.postgres_config


# -------------------- Главная асинхронная функция --------------------
async def qdrant_create_faq_async() -> bool:
    """Главная функция для создания коллекции FAQ в Qdrant.

    1. Загружает FAQ из Postgres
    2. Создаёт/сбрасывает коллекцию Qdrant
    3. Загружает FAQ в коллекцию
    4. Проверяет работу поиска с тестовым запросом
    """
    # Шаг 1: Загрузка данных из Postgres
    docs = await faq_load_from_postgres()
    if not docs:
        logger.warning("Нет данных для загрузки.")
        return False

    # Шаг 2: Сброс и создание коллекции в Qdrant
    await reset_collection(qdrant_client, QDRANT_COLLECTION)

    # Шаг 3: Загрузка данных в коллекцию
    await fill_collection_faq(docs, QDRANT_COLLECTION)

    # Шаг 4: Проверка работы поиска с тестовым запросом
    results = await retriver_hybrid_async("Абонемент", QDRANT_COLLECTION)
    logger.info(f"Найдено результатов: {len(results)}")

    # Возвращаем True если поиск вернул хотя бы один результат
    return bool(results)


# -------------------- Загрузка FAQ из Postgres --------------------
async def faq_load_from_postgres() -> list[dict[str, Any]]:
    """Загружает все записи FAQ из таблицы 'faq' в Postgres.

    Возвращает список словарей с ключами:
    channel_id, id, topic, question, answer
    """
    conn = await asyncpg.connect(**POSTGRES_CONFIG)  # Подключение к БД
    try:
        rows = await conn.fetch(
            "SELECT channel_id, id, topic, question, answer FROM faq"
        )
        # Преобразуем строки в список словарей
        return [dict(r) for r in rows]
    finally:
        await conn.close()  # Закрываем соединение


# -------------------- Загрузка FAQ в Qdrant --------------------
async def fill_collection_faq(
    docs: list[dict[str, Any]], collection_name: str, batch_size: int = 64
) -> None:
    """Загружает FAQ в коллекцию Qdrant.

    Для каждой записи создаются два типа эмбеддингов:
        - BM25 (sparse)
        - OpenAI ADA (dense)
    docs: список словарей FAQ
    collection_name: название коллекции Qdrant
    batch_size: размер батча для пакетной загрузки
    """
    logger.info(f"Загрузка {len(docs)} FAQ-записей в '{collection_name}'")

    # Разбиваем данные на батчи и отображаем прогресс
    for batch in tqdm_asyncio(batch_iterable(docs, batch_size), desc="FAQ batches"):
        # Фильтруем записи без вопросов
        filtered = [d for d in batch if d.get("question", "").strip()]
        if not filtered:
            continue

        # Получаем список вопросов
        questions = [d["question"] for d in filtered]

        # ---------------- Embeddings ----------------
        # Sparse BM25 embeddings (fastembed)
        bm25_emb = list(bm25_embedding_model.passage_embed(questions))
        # Dense OpenAI embeddings
        ada_emb = await ada_embeddings(questions)

        # ---------------- Формирование точек Qdrant ----------------
        points = [
            models.PointStruct(
                id=int(d["id"]),  # Используем id из БД как идентификатор точки
                vector={
                    "ada-embedding": ada_emb[i],  # Dense вектор
                    "bm25": bm25_emb[i].as_object(),  # Sparse вектор
                },
                payload=d,  # Сохраняем всю запись как payload
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
    asyncio.run(qdrant_create_faq_async())


# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m zena_qdrant.qdrant.qdrant_creat_faq
