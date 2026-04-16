"""Модуль общих функций для работы с qdrant."""

import asyncio
import os
import inspect
import random
from collections.abc import Iterator
from typing import Any, Awaitable, Callable, Sequence, TypeVar, Union

import httpx
from fastembed.sparse.bm25 import Bm25
from openai import AsyncOpenAI
from openai.types.create_embedding_response import CreateEmbeddingResponse
from qdrant_client import AsyncQdrantClient, models

# Свои модули
from ..zena_logging import get_logger  # type: ignore

logger = get_logger()
from ..settings import settings  # type: ignore

T = TypeVar("T")

# -------------------- Config --------------------
# Конфигурация для OpenAI, Qdrant и Postgres
OPENAI_API_KEY = settings.openai_api_key
OPENAI_PROXY = settings.openai_proxy_url  # Прокси для OpenAI (если нужен)
OPENAI_TIMEOUT = settings.openai_timeout  # Таймаут запросов к OpenAI
QDRANT_URL = settings.qdrant_url  # URL Qdrant
QDRANT_TIMEOUT = settings.qdrant_timeout  # Таймаут запросов к Qdrant

# -------------------- Clients --------------------
# Инициализация клиентов для работы с разными сервисами

# BM25 sparse embedding модель для поиска по тексту
_bm25_model_path = os.environ.get("BM25_MODEL_PATH")
bm25_embedding_model = Bm25(
    "Qdrant/bm25",
    language="russian",
    **( {"specific_model_path": _bm25_model_path} if _bm25_model_path else {}),
)

# Асинхронный клиент OpenAI с использованием httpx
openai_client = AsyncOpenAI(
    api_key=OPENAI_API_KEY,
    http_client=httpx.AsyncClient(proxy=OPENAI_PROXY, timeout=OPENAI_TIMEOUT),
)

# Асинхронный клиент Qdrant для работы с векторной базой данных
qdrant_client = AsyncQdrantClient(
    QDRANT_URL, timeout=QDRANT_TIMEOUT, check_compatibility=False
)


# -------------------- Retry helper --------------------
# Универсальная функция с повторной попыткой ТОЛЬКО для асинхронных функций
async def retry_request(
    func: Callable[..., Union[T, Awaitable[T]]],  # допускает обычный и async вызов
    *args: Any,
    retries: int = 3,
    backoff: float = 2.0,
    **kwargs: Any,
) -> T:
    """Функция повтора."""
    for attempt in range(1, retries + 1):
        try:
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result  # awaitable coroutine
            return result  # обычная функция
        except Exception as e:
            if attempt == retries:
                logger.exception("retry.exhausted", func=func.__name__, error=str(e))
                raise
            wait = backoff**attempt + random.uniform(0, 1)
            logger.warning(
                "retry.attempt", func=func.__name__, error=str(e), attempt=attempt, retries=retries, wait=round(wait, 1)
            )
            await asyncio.sleep(wait)
    assert False, "Unreachable: All attempts failed but no exception was thrown"



# -------------------- Batch helper --------------------
# Генератор для разбиения любого итерируемого объекта на батчи заданного размера
def batch_iterable(iterable: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    """Генератор для разбиения любого итерируемого объекта на батчи заданного размера."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


# -------------------- Embeddings --------------------
# Асинхронная функция для получения векторных представлений текстов
async def embed_texts(
    texts: list[str], model: str, dimensions: int | None = None
) -> list[list[float]]:
    """Асинхронная функция для получения векторных представлений текстов."""
    # Убираем пустые строки и заменяем переносы строк на пробелы
    texts = [t.replace("\n", " ") for t in texts if t and t.strip()]
    if not texts:
        return []  # если нет текста, возвращаем пустой список
    # Получаем эмбеддинги через OpenAI с повторными попытками
    response: CreateEmbeddingResponse = await retry_request(
        openai_client.embeddings.create,
        input=texts,
        model=model,
        **(
            {"dimensions": dimensions} if dimensions else {}
        ),  # передаем размерность, если указана
    )
    # Возвращаем список векторов
    return [item.embedding for item in response.data]


# Обертка для стандартной модели ada
async def ada_embeddings(
    texts: list[str], model: str = "text-embedding-ada-002"
) -> list[list[float]]:
    """Обертка для стандартной модели ada."""
    return await embed_texts(texts, model=model)


# -------------------- Reset collection --------------------
# Функция для удаления и создания коллекции в Qdrant с настройкой векторов и индексов
async def reset_collection(
    client: AsyncQdrantClient,
    collection_name: str,
    text_index_fields: list[str] | None = None,  # поля для текстового поиска
) -> None:
    """Функция для удаления и создания коллекции в Qdrant с настройкой векторов и индексов."""
    try:
        # Пробуем удалить коллекцию (если она существует)
        await client.delete_collection(collection_name)
        logger.info("qdrant.collection.deleted", collection=collection_name)
    except Exception:
        logger.warning("qdrant.collection.not_found", collection=collection_name)

    # Создаем новую коллекцию с конфигурацией HNSW и векторных пространств
    await client.create_collection(
        collection_name,
        hnsw_config=models.HnswConfigDiff(
            m=32,  # параметр HNSW: количество соседей для построения графа
            ef_construct=200,  # точность построения индекса
            full_scan_threshold=50000,  # порог для полного сканирования вместо индекса
            max_indexing_threads=4,  # количество потоков для индексации
        ),
        vectors_config={
            "ada-embedding": models.VectorParams(
                size=1536,  # размерность эмбеддинга
                distance=models.Distance.COSINE,  # метрика косинусного сходства
                datatype=models.Datatype.FLOAT16,  # тип хранения
            ),
        },
        sparse_vectors_config={
            "bm25": models.SparseVectorParams(
                modifier=models.Modifier.IDF,  # модификатор BM25
                index=models.SparseIndexParams(),  # параметры sparse индекса
            ),
        },
    )
    logger.info("qdrant.collection.created", collection=collection_name)

    # Создаем текстовые индексы для указанных полей
    if text_index_fields:
        for field in text_index_fields:
            await client.create_payload_index(
                collection_name=collection_name,
                field_name=field,
                field_schema=models.TextIndexParams(
                    type=models.TextIndexType.TEXT,
                    tokenizer=models.TokenizerType.WORD,
                    min_token_len=1,
                    max_token_len=15,
                    lowercase=True,
                ),
            )
            logger.info("qdrant.index.created", collection=collection_name, field=field)
