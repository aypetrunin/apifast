import asyncio
from typing import Any

from qdrant_client import models

from ..common import logger
from .qdrant_common import (
    qdrant_client,         # Асинхронный клиент Qdrant
    ada_embeddings,        # Функция генерации dense-векторов OpenAI (Ada)
    bm25_embedding_model,  # Sparse-векторная модель BM25 (fastembed)
    retry_request,         # Надёжный вызов с повторными попытками
)

# -------------------- Конфигурация --------------------
COLLECTION_NAME = "zena2_products_services_view"


# -------------------- Преобразование точек --------------------
def points_to_list(points) -> list[dict[str, Any]]:
    """
    Преобразует результаты запроса Qdrant (объекты ScoredPoint или Record)
    в удобный для чтения список словарей с ключами продукта.

    Аргументы:
        points: результат запроса Qdrant (может содержать поле .points)

    Возвращает:
        Список словарей, содержащих поля продукта: имя, тип, длительность, цена и т.д.
    """
    # Если пришёл объект с полем .points — извлекаем его
    if hasattr(points, "points"):
        points = points.points

    result = []
    for p in points:
        pl = p.payload  # payload — это словарь, сохранённый в точке Qdrant
        price_min, price_max = pl.get("price_min"), pl.get("price_max")

        # Формируем карточку продукта в удобном формате
        result.append({
            "product_id": pl.get("product_id"),
            "product_name": pl.get("product_name"),
            "product_type": pl.get("product_type"),
            "body_parts": pl.get("body_parts"),
            "indications_key": pl.get("indications_key"),
            "contraindications_key": pl.get("contraindications_key"),
            "duration": pl.get("duration"),
            # Форматируем цену как диапазон, если min != max
            "price": (
                f"{price_min} руб." if price_min == price_max
                else f"{price_min} - {price_max} руб."
            ) if price_min and price_max else None
        })
    return result


# -------------------- Универсальный сборщик фильтров --------------------
def make_filter(
    channel_id: int | None = None,
    indications: list[str] | None = None,
    contraindications: list[str] | None = None,
    body_parts: list[str] | None = None,
    product_type: list[str] | None = None,
    use_should: bool = False,
) -> models.Filter | None:
    """
    Формирует объект фильтра Qdrant для запросов с учётом полей:
    канал, показания, противопоказания, части тела, тип продукта.

    Аргументы:
        channel_id: фильтр по ID канала
        indications: список показаний
        contraindications: список противопоказаний
        body_parts: список частей тела
        product_type: тип продукта (например, "разовый", "абонемент")
        use_should: если True, используется мягкое соответствие (should), а не строгое (must)

    Возвращает:
        models.Filter или None, если фильтры не заданы
    """
    must, must_not, should = [], [], []

    # --- Фильтрация по каналу ---
    if channel_id:
        must.append(models.FieldCondition(
            key="channel_id",
            match=models.MatchValue(value=channel_id)
        ))

    # --- Фильтрация по показаниям ---
    if indications:
        (should if use_should else must).extend([
            models.FieldCondition(key="indications_key", match=models.MatchText(text=i))
            for i in indications
        ])

    # --- Фильтрация по частям тела ---
    if body_parts:
        must.extend([
            models.FieldCondition(key="body_parts", match=models.MatchText(text=b))
            for b in body_parts
        ])

    # --- Фильтрация по типу продукта ---
    if product_type:
        must.extend([
            models.FieldCondition(key="product_type", match=models.MatchText(text=t))
            for t in product_type
        ])

    # --- Исключение по противопоказаниям ---
    if contraindications:
        must_not.extend([
            models.FieldCondition(key="contraindications_key", match=models.MatchText(text=c))
            for c in contraindications
        ])

    # Возвращаем собранный фильтр, если есть условия
    if any([must, must_not, should]):
        return models.Filter(must=must or None, must_not=must_not or None, should=should or None)
    return None


# -------------------- Базовый поиск (только Ada embeddings) --------------------
async def retriever_product_async(
    query: str | None = None,
    indications: list[str] | None = None,
    contraindications: list[str] | None = None
) -> list[str] | None:
    """
    Выполняет поиск продуктов по текстовому запросу (через OpenAI Ada embedding)
    и фильтрам по показаниям и противопоказаниям.

    Аргументы:
        query: поисковая строка (например, "массаж лица")
        indications: фильтр по показаниям
        contraindications: фильтр по противопоказаниям

    Возвращает:
        Список найденных продуктов с кратким описанием.
    """
    query_filter = make_filter(indications=indications, contraindications=contraindications)

    async def _logic():
        if query:
            # Создаём dense-вектор OpenAI Ada
            query_vector = (await ada_embeddings([query]))[0]

            # Поиск ближайших точек в Qdrant
            res = await qdrant_client.query_points(
                collection_name=COLLECTION_NAME,
                query=query_vector,
                using="ada-embedding",
                with_payload=True,
                limit=5,
                query_filter=query_filter
            )
        else:
            # Если запроса нет — просто скроллим коллекцию
            res, _ = await qdrant_client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=query_filter,
                with_payload=True,
                limit=5
            )
        return points_to_list(res)

    # Оборачиваем вызов в retry для надёжности
    return await retry_request(_logic)


# -------------------- Гибридный поиск (Ada + BM25, RRF fusion) --------------------
async def retriever_product_hybrid_async(
    channel_id: int,
    query: str | None = None,
    indications: list[str] | None = None,
    contraindications: list[str] | None = None,
    body_parts: list[str] | None = None,
    product_type: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Гибридный поиск, объединяющий dense-векторы (OpenAI Ada)
    и sparse-векторы (BM25) с помощью Reciprocal Rank Fusion (RRF).

    Используется, если нужно объединить "понимание смысла" и "точное совпадение слов".

    Аргументы:
        channel_id: фильтр по каналу
        query: поисковая строка
        indications, contraindications, body_parts, product_type: дополнительные фильтры

    Возвращает:
        Список найденных продуктов с агрегированным рейтингом.
    """
    query_filter = make_filter(
        channel_id=channel_id,
        indications=indications,
        contraindications=contraindications,
        body_parts=body_parts,
        product_type=product_type,
        use_should=True
    )

    async def _logic():
        if query:
            # --- Генерация векторов ---
            qv_ada = (await ada_embeddings([query]))[0]
            qv_bm25 = next(bm25_embedding_model.query_embed(query))

            # --- Настройка prefetch для гибридного поиска ---
            prefetch = [
                models.Prefetch(query=qv_ada, using="ada-embedding", limit=12),
                models.Prefetch(query=models.SparseVector(**qv_bm25.as_object()), using="bm25", limit=12),
            ]

            # --- Выполнение гибридного поиска (RRF) ---
            res = await qdrant_client.query_points(
                collection_name=COLLECTION_NAME,
                prefetch=prefetch,
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                with_payload=True,
                query_filter=query_filter,
                limit=12
            )
        else:
            # --- Если текста нет — просто фильтрация по полям ---
            res, _ = await qdrant_client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=query_filter,
                with_payload=True,
                limit=12
            )
        return points_to_list(res)

    return await retry_request(_logic)


# -------------------- Гибридный поиск с FormulaQuery --------------------
async def retriever_product_hybrid_mult_async(
    channel_id: int,
    query: str | None = None,
    indications: list[str] | None = None,
    contraindications: list[str] | None = None,
    body_parts: list[str] | None = None,
    product_type: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Расширенный гибридный поиск, использующий FormulaQuery
    для задания кастомных весов при объединении скоринговых факторов.

    Подходит для бустинга релевантности по дополнительным полям.

    Аргументы:
        channel_id: ID канала
        query: поисковая строка
        indications, contraindications, body_parts, product_type: фильтры

    Возвращает:
        Список найденных продуктов с учётом пользовательских весов.
    """
    query_filter = make_filter(
        channel_id=channel_id,
        indications=indications,
        contraindications=contraindications,
        body_parts=body_parts,
        product_type=product_type
    )

    async def _logic():
        if query:
            qv_ada = (await ada_embeddings([query]))[0]
            qv_bm25 = next(bm25_embedding_model.query_embed(query))

            prefetch = [
                models.Prefetch(query=qv_ada, using="ada-embedding", limit=10),
                models.Prefetch(query=models.SparseVector(**qv_bm25.as_object()), using="bm25", limit=10),
            ]

            # --- FormulaQuery: объединение с весами ---
            formula = models.FormulaQuery(
                formula=models.SumExpression(sum=[
                    "$score",
                    # Бустинг по мультипликативным признакам
                    models.MultExpression(mult=[
                        0.3,
                        models.FieldCondition(
                            key="mult_score_boosting",
                            match=models.MatchAny(any=["mult_1"])
                        )
                    ]),
                    models.MultExpression(mult=[
                        0.2,
                        models.FieldCondition(
                            key="mult_score_boosting",
                            match=models.MatchAny(any=["mult_2"])
                        )
                    ])
                ])
            )

            res = await qdrant_client.query_points(
                collection_name=COLLECTION_NAME,
                prefetch=prefetch,
                query=formula,
                with_payload=True,
                query_filter=query_filter,
                limit=10
            )
        else:
            res, _ = await qdrant_client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=query_filter,
                with_payload=True,
                limit=10
            )
        return points_to_list(res)

    return await retry_request(_logic)


# -------------------- Тестовый запуск --------------------
if __name__ == "__main__":
    async def main():
        """
        Пример тестового вызова гибридного поиска.
        Ищет массажные услуги по фильтрам и запросу.
        """
        results = await retriever_product_hybrid_async(
            channel_id=2,
            query="массаж",
            indications=["отечность"],
            contraindications=["высокое"],
            body_parts=["тело"],
            # product_type=["разовый"]
        )
        logger.info(f"Результаты: {len(results)} элементов")
        # logger.info(results[:3])

    asyncio.run(main())



# cd /home/copilot_superuser/petrunin/mcp
# uv run python -m zena_qdrant.qdrant.qdrant_retriver_product