import asyncio
from typing import Any

import asyncpg

from ..common import logger  # type: ignore
from ..settings import settings  # type: ignore
from .qdrant_create_services import qdrant_create_services_async
from .qdrant_retriever_faq_services import retriver_hybrid_async

QDRANT_COLLECTION_TEMP = settings.qdrant_collection_temp
POSTGRES_CONFIG = settings.postgres_config


async def _try_enable_amcheck(conn: asyncpg.Connection) -> bool:
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS amcheck;")
        return True
    except Exception as e:
        logger.warning(f"amcheck недоступен (нет прав/расширения): {e}")
        return False


async def _index_exists(conn: asyncpg.Connection, index_regclass: str) -> bool:
    row = await conn.fetchrow("SELECT to_regclass($1) IS NOT NULL AS ok;", index_regclass)
    return bool(row and row["ok"])


async def _btree_index_check(conn: asyncpg.Connection, index_regclass: str) -> tuple[bool, str | None]:
    try:
        # thorough = true
        await conn.execute("SELECT bt_index_check($1::regclass, true);", index_regclass)
        return True, None
    except Exception as e:
        return False, str(e)


async def _reindex_index(conn: asyncpg.Connection, index_regclass: str) -> None:
    await conn.execute(f"REINDEX INDEX {index_regclass};")


async def _reindex_table(conn: asyncpg.Connection, table_fqn: str) -> None:
    await conn.execute(f"REINDEX TABLE {table_fqn};")


async def _check_and_fix_products_indexes(conn: asyncpg.Connection) -> None:
    """
    Проверяет целостность btree-индексов products (через amcheck) и чинит REINDEX-ом при проблемах.
    Выполнять ДО бизнес-транзакции.
    """
    if not await _try_enable_amcheck(conn):
        return

    # 1) Приоритетно проверим уникальный индекс по article (частый виновник)
    primary_suspect = "public.products_article_key"
    suspects: list[str] = []

    if await _index_exists(conn, primary_suspect):
        suspects.append(primary_suspect)

    # 2) Добавим остальные btree-индексы products (на случай более широкой порчи)
    rows = await conn.fetch(
        """
        SELECT (quote_ident(n.nspname) || '.' || quote_ident(ic.relname)) AS idx
        FROM pg_index i
        JOIN pg_class c  ON c.oid = i.indrelid
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_am am ON am.oid = ic.relam
        WHERE n.nspname = 'public'
          AND c.relname = 'products'
          AND am.amname = 'btree';
        """
    )
    for r in rows:
        idx = r["idx"]
        if idx not in suspects:
            suspects.append(idx)

    if not suspects:
        logger.info("btree-индексы для public.products не найдены — проверка пропущена")
        return

    bad: list[tuple[str, str]] = []

    for idx in suspects:
        ok, err = await _btree_index_check(conn, idx)
        if ok:
            continue
        bad.append((idx, err or "unknown error"))
        logger.error(f"Индекс поврежден/подозрителен: {idx}. Ошибка: {err}")

    if not bad:
        logger.info("Проверка индексов public.products: OK")
        return

    # Сначала чиним точечно каждый битый индекс
    for idx, _ in bad:
        try:
            logger.warning(f"REINDEX INDEX {idx}")
            await _reindex_index(conn, idx)
        except Exception as e:
            logger.error(f"REINDEX INDEX не удался для {idx}: {e}")

    # Перепроверка: если что-то осталось — REINDEX TABLE целиком
    still_bad: list[str] = []
    for idx, _ in bad:
        ok, err = await _btree_index_check(conn, idx)
        if not ok:
            still_bad.append(idx)
            logger.error(f"После REINDEX INDEX всё ещё проблема: {idx}. Ошибка: {err}")

    if still_bad:
        logger.warning(
            f"Не все индексы починились точечно ({len(still_bad)}). Делаю REINDEX TABLE public.products"
        )
        await _reindex_table(conn, "public.products")

    # Обновим статистику
    try:
        await conn.execute("ANALYZE public.products;")
    except Exception as e:
        logger.warning(f"ANALYZE public.products не удался: {e}")


async def update_products_services(
    channel_id: int,
    collection_name: str = QDRANT_COLLECTION_TEMP,
    qdrant_create_services: bool = True,
    max_parallel: int = 10,
) -> bool:
    logger.info("update_products_services")
    logger.info(f"Начало обновления 'products_services' для channel_id={channel_id}")

    if qdrant_create_services:
        logger.info("Создания вспомогательной векторной.")
        result = await qdrant_create_services_async(collection_name, channel_id)
        if not result:
            logger.error(
                f"Ошибка создания вспомогательной векторной базы '{QDRANT_COLLECTION_TEMP}' для channel_id={channel_id}"
            )

    conn = await asyncpg.connect(**POSTGRES_CONFIG)
    semaphore = asyncio.Semaphore(max_parallel)
    try:
        # ВАЖНО: до транзакции
        await _check_and_fix_products_indexes(conn)

        async with conn.transaction():
            logger.info("Получение id сервисов для удаления связанных записей.")
            service_ids = await conn.fetch(
                "SELECT id FROM services WHERE channel_id = $1", channel_id
            )
            ids_to_delete = [record["id"] for record in service_ids]

            logger.info("Удаление связанных записей из products_services.")
            if ids_to_delete:
                await conn.execute(
                    "DELETE FROM products_services WHERE service_id = ANY($1::int[])",
                    ids_to_delete,
                )

            logger.info("Получение продуктов канала.")
            products = await conn.fetch(
                "SELECT product_full_name as product_name, article FROM products WHERE channel_id = $1",
                channel_id,
            )

            logger.info("Параллельный сбор service_id для продуктов с ограничением")
            tasks = [
                _fetch_service_id(product, channel_id, semaphore)
                for product in products
            ]
            results = await asyncio.gather(*tasks)

            logger.info("Фильтрация успешных результатов")
            insert_tuples = [res for res in results if res is not None]

            logger.info("Вставка новых записей в products_services")
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



# """Модуль реализует обновление таблицы products_services."""

# import asyncio
# from typing import Any

# import asyncpg

# from ..common import logger  # type: ignore
# from ..settings import settings  # type: ignore
# from .qdrant_create_services import qdrant_create_services_async
# from .qdrant_retriever_faq_services import retriver_hybrid_async

# QDRANT_COLLECTION_TEMP = settings.qdrant_collection_temp
# POSTGRES_CONFIG = settings.postgres_config


# async def update_products_services(
#     channel_id: int,
#     collection_name: str = QDRANT_COLLECTION_TEMP,
#     qdrant_create_services: bool = True,
#     max_parallel: int = 10,
# ) -> bool:
#     """Асинхронное обновление таблицы products_services для заданного канала.

#     Процесс:
#     1. Опциональное создание вспомогательной векторной базы Qdrant сервисов.
#     2. Получение id всех сервисов и удаление связанных записей в products_services.
#     3. Получение продуктов, получение service_id для каждого продукта параллельно с ограничением max_parallel.
#     4. Вставка новых связей product-service в таблицу products_services.
#     5. Логирование и возврат результата обновления.
#     """
#     logger.info("update_products_services")
#     logger.info(f"Начало обновления 'products_services' для channel_id={channel_id}")

#     if qdrant_create_services:
#         # Создания вспомогательной векторной.
#         logger.info("Создания вспомогательной векторной.")
#         result = await qdrant_create_services_async(collection_name, channel_id)
#         if not result:
#             logger.error(
#                 f"Ошибка создания вспомогательной векторной базы '{QDRANT_COLLECTION_TEMP}' для channel_id={channel_id}"
#             )

#     conn = await asyncpg.connect(**POSTGRES_CONFIG)
#     semaphore = asyncio.Semaphore(max_parallel)
#     try:
#         async with conn.transaction():
#             # Получение id сервисов для удаления связанных записей
#             logger.info("Получение id сервисов для удаления связанных записей.")
#             service_ids = await conn.fetch(
#                 "SELECT id FROM services WHERE channel_id = $1", channel_id
#             )
#             ids_to_delete = [record["id"] for record in service_ids]

#             # Удаление связанных записей из products_services
#             logger.info("Удаление связанных записей из products_services.")
#             if ids_to_delete:
#                 await conn.execute(
#                     "DELETE FROM products_services WHERE service_id = ANY($1::int[])",
#                     ids_to_delete,
#                 )

#             # Получение продуктов канала
#             logger.info("Получение продуктов канала.")
#             products = await conn.fetch(
#                 "SELECT product_full_name as product_name, article FROM products WHERE channel_id = $1",
#                 channel_id,
#             )

#             logger.info(products)
#             # Параллельный сбор service_id для продуктов с ограничением
#             logger.info("Параллельный сбор service_id для продуктов с ограничением")
#             tasks = [
#                 _fetch_service_id(product, channel_id, semaphore)
#                 for product in products
#             ]
#             results = await asyncio.gather(*tasks)

#             # Фильтрация успешных результатов
#             logger.info("Фильтрация успешных результатов")
#             insert_tuples = [res for res in results if res is not None]
#             print(insert_tuples)

#             # Вставка новых записей в products_services
#             logger.info("Вставка новых записей в products_services")
#             if insert_tuples:
#                 await conn.executemany(
#                     "INSERT INTO products_services (article_id, service_id) VALUES ($1, $2)",
#                     insert_tuples,
#                 )

#         logger.info(
#             f"Обновление 'products_services' успешно завершено для channel_id={channel_id}"
#         )
#         return True

#     except Exception as e:
#         logger.error(
#             f"Ошибка обновления 'products_services' для channel_id={channel_id}: {e}"
#         )
#         return False

#     finally:
#         await conn.close()


# async def _fetch_service_id(
#     product: dict[str, Any],
#     channel_id: int,
#     semaphore: asyncio.Semaphore,
# ) -> tuple[str, int] | None:
#     """Получает service_id для данного продукта из векторной базы Qdrant.

#     Использует semaphore для ограничения количества параллельных запросов.

#     :param product: словарь с информацией о продукте (product_name, article)
#     :param channel_id: идентификатор канала
#     :param semaphore: семафор для ограничения параллелизма
#     :return: кортеж (article, service_id) или None в случае ошибки/отсутствия результата
#     """
#     async with semaphore:
#         try:
#             result = await retriver_hybrid_async(
#                 query=product["product_name"],
#                 database_name=QDRANT_COLLECTION_TEMP,
#                 channel_id=channel_id,
#                 hybrid=False,
#                 limit=1,
#             )
#             if result:
#                 return (product["article"], result[0]["id"])
#         except Exception as e:
#             logger.error(
#                 f"Ошибка при получении service_id для продукта {product['article']}: {e}"
#             )
#         return None


if __name__ == "__main__":
    result = asyncio.run(
        update_products_services(
            channel_id=1,
        )
    )

# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update_postgres_qdrant.postgres_update_products_services
