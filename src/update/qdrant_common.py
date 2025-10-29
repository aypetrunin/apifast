"""Модуль общих функций для работы с qdrant."""

import asyncio
import inspect
import random
from typing import Awaitable, Callable, TypeVar, Any, Union, Sequence
from collections.abc import Iterator

import httpx
from fastembed.sparse.bm25 import Bm25
from openai import AsyncOpenAI
from openai.types.create_embedding_response import CreateEmbeddingResponse
from qdrant_client import AsyncQdrantClient, models

from ..common import logger
from ..settings import settings

T = TypeVar('T')

# -------------------- Config --------------------
# Конфигурация для OpenAI, Qdrant и Postgres
OPENAI_API_KEY = settings.openai_api_key
OPENAI_PROXY = settings.openai_proxy_url  # Прокси для OpenAI (если нужен)
OPENAI_TIMEOUT = settings.openai_timeout  # Таймаут запросов к OpenAI
QDRANT_URL = settings.qdrant_url  # URL Qdrant
QDRANT_TIMEOUT = settings.qdrant_timeout  # Таймаут запросов к Qdrant

# Конфигурация для подключения к PostgreSQL
POSTGRES_CONFIG = settings.postgres_config

# -------------------- Clients --------------------
# Инициализация клиентов для работы с разными сервисами

# BM25 sparse embedding модель для поиска по тексту
bm25_embedding_model = Bm25("Qdrant/bm25", language="russian")

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
    for attempt in range(1, retries + 1):
        try:
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                return await result  # awaitable coroutine
            return result  # обычная функция
        except Exception as e:
            if attempt == retries:
                logger.exception(f"Последняя неудачная попытка {func.__name__}: {e}")
                raise
            wait = backoff**attempt + random.uniform(0, 1)
            logger.warning(
                f"Ошибка в {func.__name__}: {e} | попытка {attempt}/{retries} — повтор через {wait:.1f}s"
            )
            await asyncio.sleep(wait)
    assert False, "Unreachable: All attempts failed but no exception was thrown"

# Универсальная функция с повторной попыткой для асинхронных/синхронных функций
# async def retry_request(
#     func: Callable[..., Awaitable[T]],  # функция, которую нужно выполнить
#     *args: Any,
#     retries: int = 3,  # количество попыток
#     backoff: float = 2.0,  # коэффициент экспоненциального backoff
#     **kwargs: Any,
# ) -> T:
#     """Универсальная функция с повторной попыткой для асинхронных/синхронных функций."""
#     for attempt in range(1, retries + 1):
#         try:
#             # Проверяем, является ли функция асинхронной
#             if inspect.iscoroutinefunction(func):
#                 return await func(*args, **kwargs)
#             return func(*args, **kwargs)
#         except Exception as e:
#             # Если это последняя попытка — логируем как ошибку и пробрасываем
#             if attempt == retries:
#                 logger.exception(f"Последняя неудачная попытка {func.__name__}: {e}")
#                 raise
#             # Вычисляем время ожидания с небольшой случайной погрешностью
#             wait = backoff**attempt + random.uniform(0, 1)
#             logger.warning(
#                 f"Ошибка в {func.__name__}: {e} | попытка {attempt}/{retries} — повтор через {wait:.1f}s"
#             )
#             await asyncio.sleep(wait)
#     # Добавьте этот return, чтобы у функции всегда был return:
#     assert False, "Unreachable: All attempts failed but no exception was thrown"

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
        texts: list[str],
        model: str = "text-embedding-ada-002"
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
        logger.info(f'Коллекция "{collection_name}" удалена.')
    except Exception:
        logger.warning(f'Коллекция "{collection_name}" не найдена или ошибка удаления.')

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
    logger.info(f'Коллекция "{collection_name}" создана.')

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
            logger.info(f'Индекс "{field}" создан.')


    # if text_index_fields:
    #     default_text_index_params = {
    #         "type": "text",
    #         "tokenizer": models.TokenizerType.WORD,
    #         "min_token_len": 1,
    #         "max_token_len": 15,
    #         "lowercase": True,
    #     }
    #     for field in text_index_fields:
    #         await client.create_payload_index(
    #             collection_name=collection_name,
    #             field_name=field,
    #             field_schema=models.TextIndexParams(**default_text_index_params),
    #         )
    #         logger.info(f'Индекс "{field}" создан.')
