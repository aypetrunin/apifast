"""Модуль реализует обновление таблицы products_services."""

import asyncio
from typing import Any

import asyncpg

from ..common import logger  # type: ignore
from ..settings import settings  # type: ignore
from .qdrant_create_services import qdrant_create_services_async
from .qdrant_retriever_faq_services import retriver_hybrid_async

QDRANT_COLLECTION_TEMP = settings.qdrant_collection_temp
POSTGRES_CONFIG = settings.postgres_config


async def update_products_services(
    channel_id: int,
    collection_name: str = QDRANT_COLLECTION_TEMP,
    qdrant_create_services: bool = True,
    max_parallel: int = 10,
) -> bool:
    """Асинхронное обновление таблицы products_services для заданного канала.

    Процесс:
    1. Опциональное создание вспомогательной векторной базы Qdrant сервисов.
    2. Получение id всех сервисов и удаление связанных записей в products_services.
    3. Получение продуктов, получение service_id для каждого продукта параллельно с ограничением max_parallel.
    4. Вставка новых связей product-service в таблицу products_services.
    5. Логирование и возврат результата обновления.
    """
    logger.info(f"Начало обновления 'products_services' для channel_id={channel_id}")

    if qdrant_create_services:
        # Создания вспомогательной векторной.
        result = await qdrant_create_services_async(collection_name, channel_id)
        if not result:
            logger.error(
                f"Ошибка создания вспомогательной векторной базы '{QDRANT_COLLECTION_TEMP}' для channel_id={channel_id}"
            )

    conn = await asyncpg.connect(**POSTGRES_CONFIG)
    semaphore = asyncio.Semaphore(max_parallel)
    try:
        async with conn.transaction():
            # Получение id сервисов для удаления связанных записей
            service_ids = await conn.fetch(
                "SELECT id FROM services WHERE channel_id = $1", channel_id
            )
            ids_to_delete = [record["id"] for record in service_ids]

            # Удаление связанных записей из products_services
            if ids_to_delete:
                await conn.execute(
                    "DELETE FROM products_services WHERE service_id = ANY($1::int[])",
                    ids_to_delete,
                )

            # Получение продуктов канала
            products = await conn.fetch(
                "SELECT product_full_name as product_name, article FROM products WHERE channel_id = $1",
                channel_id,
            )
            # Параллельный сбор service_id для продуктов с ограничением
            tasks = [
                _fetch_service_id(product, channel_id, semaphore)
                for product in products
            ]
            results = await asyncio.gather(*tasks)

            # Фильтрация успешных результатов
            insert_tuples = [res for res in results if res is not None]

            # Вставка новых записей в products_services
            if insert_tuples:
                await conn.executemany(
                    "INSERT INTO products_services (article_id, service_id) VALUES ($1, $2)",
                    insert_tuples,
                )

        logger.info(
            f"Обновление 'products_services' успешно завершено для channel_id={channel_id}"
        )
        return True

    except Exception as e:
        logger.error(
            f"Ошибка обновления 'products_services' для channel_id={channel_id}: {e}"
        )
        return False

    finally:
        await conn.close()


async def _fetch_service_id(
    product: dict[str, Any],
    channel_id: int,
    semaphore: asyncio.Semaphore,
) -> tuple[str, int] | None:
    """Получает service_id для данного продукта из векторной базы Qdrant.

    Использует semaphore для ограничения количества параллельных запросов.

    :param product: словарь с информацией о продукте (product_name, article)
    :param channel_id: идентификатор канала
    :param semaphore: семафор для ограничения параллелизма
    :return: кортеж (article, service_id) или None в случае ошибки/отсутствия результата
    """
    async with semaphore:
        try:
            result = await retriver_hybrid_async(
                query=product["product_name"],
                database_name=QDRANT_COLLECTION_TEMP,
                channel_id=channel_id,
                hybrid=False,
                limit=1,
            )
            if result:
                return (product["article"], result[0]["id"])
        except Exception as e:
            logger.error(
                f"Ошибка при получении service_id для продукта {product['article']}: {e}"
            )
        return None


if __name__ == "__main__":
    result = asyncio.run(
        update_products_services(
            channel_id=1,
        )
    )

# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update_postgres_qdrant.postgres_update_products_services
