"""Модуль в котором реализованы ретриверы по коллекции 'zena2_products_services_view'."""
import asyncio
from typing import Any

from qdrant_client import models
from qdrant_client.conversions.common_types import Record, ScoredPoint
from qdrant_client.http.models import (
    FieldCondition,
    Filter,
    HasIdCondition,
    HasVectorCondition,
    IsEmptyCondition,
    IsNullCondition,
    MatchText,
    MatchValue,
    NestedCondition,
)

from ..settings import settings  # type: ignore
from ..common import logger
from .qdrant_common import (
    ada_embeddings,  # Функция генерации dense-векторов OpenAI (Ada)
    bm25_embedding_model,  # Sparse-векторная модель BM25 (fastembed)
    qdrant_client,  # Асинхронный клиент Qdrant
    retry_request,  # Надёжный вызов с повторными попытками
)

# -------------------- Конфигурация --------------------
COLLECTION_NAME = settings.qdrant_collection_products


# -------------------- Преобразование точек --------------------
def points_to_list(points: list[Record] | list[ScoredPoint]) -> list[dict[str, Any]]:
    """Преобразует результаты запроса Qdrant.

    Преобразует результаты запроса Qdrant (объекты ScoredPoint или Record)
    в удобный для чтения список словарей с ключами продукта.

    Аргументы:
        points: результат запроса Qdrant (может содержать поле .points)

    Возвращает:
        Список словарей, содержащих поля продукта: имя, тип, длительность, цена и т.д.
    """
    # logger.info(f"points:\n{points}")
    # logger.info(f"hasattr(points, 'points'): {hasattr(points, 'points')}")
    # Определяем тип ScoredPoint по наличию атрибута points.
    if hasattr(points, "points"):
        points = points.points

    result = []
    for p in points:
        pl = p.payload or {}  # payload — это словарь, сохранённый в точке Qdrant
        price_min, price_max = pl.get("price_min"), pl.get("price_max")

        # Формируем карточку продукта в удобном формате
        result.append(
            {
                "product_id": pl.get("product_id"),
                "product_name": pl.get("product_name"),
                "product_type": pl.get("product_type"),
                "body_parts": pl.get("body_parts"),
                "indications_key": pl.get("indications_key"),
                "contraindications_key": pl.get("contraindications_key"),
                "duration": pl.get("duration"),
                # Форматируем цену как диапазон, если min != max
                "price": (
                    f"{price_min} руб."
                    if price_min == price_max
                    else f"{price_min} - {price_max} руб."
                )
                if price_min is not None and price_max is not None
                else None,
            }
        )
    # logger.info(result)
    return result


# -------------------- Универсальный сборщик фильтров --------------------
# def make_filter(
#     channel_id: int | None = None,
#     indications: list[str] | None = None,
#     contraindications: list[str] | None = None,
#     body_parts: list[str] | None = None,
#     product_type: list[str] | None = None,
#     use_should: bool = False,
# ) -> models.Filter | None:
#     """Формирует объект фильтра Qdrant для запросов.

#     Аргументы:
#         channel_id: фильтр по ID канала
#         indications: список показаний
#         contraindications: список противопоказаний
#         body_parts: список частей тела
#         product_type: тип продукта (например, "разовый", "абонемент")
#         use_should: если True, используется мягкое соответствие (should), а не строгое (must)

#     Возвращает:
#         models.Filter или None, если фильтры не заданы
#     """
#     must, must_not, should = [], [], []

#     # --- Фильтрация по каналу ---
#     if channel_id:
#         must.append(
#             models.FieldCondition(
#                 key="channel_id", match=models.MatchValue(value=channel_id)
#             )
#         )

#     # --- Фильтрация по показаниям ---
#     if indications:
#         (should if use_should else must).extend(
#             [
#                 models.FieldCondition(
#                     key="indications_key", match=models.MatchText(text=i)
#                 )
#                 for i in indications
#             ]
#         )

#     # --- Фильтрация по частям тела ---
#     if body_parts:
#         must.extend(
#             [
#                 models.FieldCondition(key="body_parts", match=models.MatchText(text=b))
#                 for b in body_parts
#             ]
#         )

#     # --- Фильтрация по типу продукта ---
#     if product_type:
#         must.extend(
#             [
#                 models.FieldCondition(
#                     key="product_type", match=models.MatchText(text=t)
#                 )
#                 for t in product_type
#             ]
#         )

#     # --- Исключение по противопоказаниям ---
#     if contraindications:
#         must_not.extend(
#             [
#                 models.FieldCondition(
#                     key="contraindications_key", match=models.MatchText(text=c)
#                 )
#                 for c in contraindications
#             ]
#         )

#     # Возвращаем собранный фильтр, если есть условия
#     if any([must, must_not, should]):
#         return models.Filter(
#             must=must or None, must_not=must_not or None, should=should or None
#         )
#     return None


def make_filter(
    channel_id: int | None = None,
    indications: list[str] | None = None,
    contraindications: list[str] | None = None,
    body_parts: list[str] | None = None,
    product_type: list[str] | None = None,
    use_should: bool = False,
) -> Filter | None:
    """Формирует объект фильтра Qdrant для запросов.

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
    must: list[
        FieldCondition
        | IsEmptyCondition
        | IsNullCondition
        | HasIdCondition
        | HasVectorCondition
        | NestedCondition
        | Filter
    ] = []
    must_not: list[
        FieldCondition
        | IsEmptyCondition
        | IsNullCondition
        | HasIdCondition
        | HasVectorCondition
        | NestedCondition
        | Filter
    ] = []
    should: list[
        FieldCondition
        | IsEmptyCondition
        | IsNullCondition
        | HasIdCondition
        | HasVectorCondition
        | NestedCondition
        | Filter
    ] = []

    if channel_id:
        must.append(
            FieldCondition(key="channel_id", match=MatchValue(value=int(channel_id)))
        )

    if indications:
        (should if use_should else must).extend(
            [
                FieldCondition(key="indications_key", match=MatchText(text=i))
                for i in indications
            ]
        )

    if body_parts:
        must.extend(
            [
                FieldCondition(key="body_parts", match=MatchText(text=b))
                for b in body_parts
            ]
        )

    if product_type:
        must.extend(
            [
                FieldCondition(key="product_type", match=MatchText(text=t))
                for t in product_type
            ]
        )

    if contraindications:
        must_not.extend(
            [
                FieldCondition(key="contraindications_key", match=MatchText(text=c))
                for c in contraindications
            ]
        )

    if any([must, must_not, should]):
        return Filter(
            must=must if must else None,
            must_not=must_not if must_not else None,
            should=should if should else None,
        )
    return None


# -------------------- Базовый поиск (только Ada embeddings) --------------------
async def retriever_product_async(
    query: str | None = None,
    indications: list[str] | None = None,
    contraindications: list[str] | None = None,
) -> list[str] | None:
    """Поиск продуктов/услуг.

    Выполняет поиск продуктов по текстовому запросу (через OpenAI Ada embedding)
    и фильтрам по показаниям и противопоказаниям.

    Аргументы:
        query: поисковая строка (например, "массаж лица")
        indications: фильтр по показаниям
        contraindications: фильтр по противопоказаниям

    Возвращает:
        Список найденных продуктов с кратким описанием.
    """
    query_filter = make_filter(
        indications=indications, contraindications=contraindications
    )

    async def _logic() -> list[dict[str, Any]]:
        res: list[ScoredPoint] | list[Record]
        if query:
            # Создаём dense-вектор OpenAI Ada
            query_vector = (await ada_embeddings([query]))[0]

            # Поиск ближайших точек в Qdrant res: list[ScoredPoint]
            res = await qdrant_client.query_points(
                collection_name=COLLECTION_NAME,
                query=query_vector,
                using="ada-embedding",
                with_payload=True,
                limit=5,
                query_filter=query_filter,
            )
        else:
            # Если запроса нет — просто скроллим коллекцию res: list[Record]
            res, _ = await qdrant_client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=query_filter,
                with_payload=True,
                limit=5,
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
    """Гибридный поиск.

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
        use_should=True,
    )
    logger.info(f"query_filter: {query_filter}")
    async def _logic() -> list[dict[str, Any]]:
        if query:
            # --- Генерация векторов ---
            qv_ada = (await ada_embeddings([query]))[0]
            qv_bm25 = next(bm25_embedding_model.query_embed(query))

            # --- Настройка prefetch для гибридного поиска ---
            prefetch = [
                models.Prefetch(query=qv_ada, using="ada-embedding", limit=12),
                models.Prefetch(
                    query=models.SparseVector(**qv_bm25.as_object()),
                    using="bm25",
                    limit=12,
                ),
            ]
            res: list[ScoredPoint] | list[Record]
            # --- Выполнение гибридного поиска (RRF) ---
            res = await qdrant_client.query_points(
                collection_name=COLLECTION_NAME,
                prefetch=prefetch,
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                with_payload=True,
                query_filter=query_filter,
                limit=12,
            )
        else:
            # --- Если текста нет — просто фильтрация по полям ---
            res, _ = await qdrant_client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=query_filter,
                with_payload=True,
                limit=12,
            )
        return points_to_list(res)

    return await retry_request(_logic)


if __name__ == "__main__":
    # Асинхронный запуск основной функции
    asyncio.run(
        retriever_product_hybrid_async(
            channel_id=1,
            query='массаж',
        )
    )


# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update.qdrant_retriever_product
