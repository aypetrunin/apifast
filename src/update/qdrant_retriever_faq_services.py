import os
import asyncio

from qdrant_client import models

from ..common import logger
from ..settings import settings
from .qdrant_common import (
    qdrant_client,         # Асинхронный клиент Qdrant
    ada_embeddings,        # Функция генерации dense-векторов OpenAI (Ada)
    bm25_embedding_model,  # Sparse-векторная модель BM25 (fastembed)
    retry_request,         # Обёртка для надёжного выполнения с повторными попытками
)

# ===============================================================
# 🔧 Конфигурация коллекций Qdrant
# ===============================================================
QDRANT_COLLECTION_FAQ = settings.qdrant_collection_faq
QDRANT_COLLECTION_SERVICES = settings.qdrant_collection_services

# ---------------------------------------------------------------
# 📦 Маппинг полей для каждой коллекции
# ---------------------------------------------------------------
DATABASE_FIELDS = {
    QDRANT_COLLECTION_FAQ: [
        "question",        # Текст вопроса
        "answer"           # Текст ответа
    ],
    QDRANT_COLLECTION_SERVICES: [
        "services_name",               # Название услуги
        "body_parts",                  # Части тела, на которые воздействует услуга
        "description",                 # Описание услуги
        "contraindications",           # Противопоказания
        "indications",                 # Показания
        "pre_session_instructions"     # Инструкции перед сеансом
    ],
    'zena2_services_key': [
        "id",        # Текст вопроса
        "services_name"           # Текст ответа
    ],
}


# ===============================================================
# 🔄 Преобразование точек (результатов Qdrant) в словари
# ===============================================================
async def points_to_dict(
        points: list[models.PointStruct],
        database_name: str
) -> list[dict]:
    """
    Преобразует список объектов Qdrant (PointStruct) в список словарей с данными из payload.

    Аргументы:
        points: список точек из результата запроса Qdrant
        database_name: имя коллекции, чтобы определить нужные поля

    Возвращает:
        Список словарей, содержащих только релевантные поля (по коллекции).
    """
    fields = DATABASE_FIELDS.get(database_name, [])
    result = []
    for point in points:
        payload = {field: point.payload.get(field) for field in fields}
        payload["id"] = point.id  # добавляем ID точки
        result.append(payload)
    return result


# ===============================================================
# 🔍 Универсальный асинхронный поисковик с поддержкой гибридного режима
# ===============================================================
async def retriver_hybrid_async(
    query: str,
    database_name: str,
    channel_id: int | None = None,
    hybrid: bool = True,
    limit: int = 5
) -> list[dict]:
    """
    Асинхронный поиск в Qdrant.

    Асинхронный поиск в Qdrant с поддержкой:
      • dense-векторов (OpenAI Ada)
      • sparse-векторов (BM25)
      • гибридного объединения (RRF fusion)
      • надёжных повторных попыток через retry_request

    Аргументы:
        query: текст поискового запроса
        database_name: имя коллекции (FAQ или Services)
        channel_id: фильтр по ID канала (опционально)
        hybrid: если True — используется гибридный поиск (Ada + BM25)
        limit: количество возвращаемых результатов

    Возвращает:
        Список словарей с найденными объектами из Qdrant.
    """

    async def _retriever_logic():
        """Логика ретривера.
        """
        # -------------------------------------------------------
        # 1️⃣ Генерация dense-вектора через OpenAI Ada
        # -------------------------------------------------------
        query_vector = (await ada_embeddings([query]))[0]

        # -------------------------------------------------------
        # 2️⃣ Генерация sparse-вектора BM25, если включён гибрид
        # -------------------------------------------------------
        if hybrid:
            query_bm25 = next(bm25_embedding_model.query_embed(query))

        # -------------------------------------------------------
        # 3️⃣ Формируем фильтр по channel_id (если задан)
        # -------------------------------------------------------
        query_filter = None
        if channel_id:
            query_filter = models.Filter(
                must=[
                    models.FieldCondition(
                        key="channel_id",
                        match=models.MatchValue(value=channel_id)
                    )
                ]
            )

        # -------------------------------------------------------
        # 4️⃣ Выполнение поиска в Qdrant
        # -------------------------------------------------------
        if hybrid:
            # --- Гибридный режим: объединяем Ada и BM25 ---
            prefetch = [
                models.Prefetch(query=query_vector, using="ada-embedding", limit=limit),
                models.Prefetch(
                    query=models.SparseVector(**query_bm25.as_object()),
                    using="bm25",
                    limit=limit
                )
            ]
            response = await qdrant_client.query_points(
                collection_name=database_name,
                prefetch=prefetch,
                query=models.FusionQuery(fusion=models.Fusion.RRF),  # Reciprocal Rank Fusion
                query_filter=query_filter,
                with_payload=True,
                limit=limit
            )
        else:
            # --- Обычный dense-поиск (только Ada) ---
            response = await qdrant_client.query_points(
                collection_name=database_name,
                query=query_vector,
                using="ada-embedding",
                query_filter=query_filter,
                with_payload=True,
                limit=limit
            )

        # -------------------------------------------------------
        # 5️⃣ Преобразуем результаты в читаемый список
        # -------------------------------------------------------
        return await points_to_dict(response.points, database_name)

    # -------------------------------------------------------
    # 🔁 Оборачиваем вызов в retry_request для устойчивости
    # -------------------------------------------------------
    return await retry_request(_retriever_logic)


# ===============================================================
# 🧪 Тестовый запуск для проверки работы retriever
# ===============================================================
if __name__ == "__main__":
    async def main():
        """
        Тестовый пример поиска в двух коллекциях Qdrant.
        
        1. FAQ — поиск по тексту вопроса/ответа
        2. Services — поиск по услугам с фильтрацией по каналу
        """
        # --- Поиск по базе FAQ ---
        results_faq = await retriver_hybrid_async(
            query="Абонент",
            database_name=QDRANT_COLLECTION_FAQ,
            channel_id=2
        )
        logger.info("📘 FAQ results:")
        logger.info(results_faq)

        # --- Поиск по базе услуг ---
        results_services = await retriver_hybrid_async(
            query="Тейпирование",
            database_name=QDRANT_COLLECTION_SERVICES,
            channel_id=2
        )
        logger.info("💆 Services results:")
        logger.info(results_services)

    # Запускаем асинхронный тест
    asyncio.run(main())

 

# cd /home/copilot_superuser/petrunin/mcp
#  uv run python -m zena_qdrant.qdrant.qdrant_retriver_faq_services