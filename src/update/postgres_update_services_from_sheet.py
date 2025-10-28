"""Модуль обновления таблицы services в postgres из GoogleSheet."""

import asyncio

import asyncpg

from ..common import logger
from ..settings import settings
from .google_sheet_reader import UniversalGoogleSheetReader


async def update_services_from_sheet(
    channel_id: int, sheet_name: str = "services"
) -> bool:
    """Асинхронное обновление данных услуг (services) из Google Sheets для заданного канала.

    Алгоритм работы:
    1. Получение URL Google Sheets из базы для channel_id.
    2. Чтение всех строк листа sheet_name.
    3. Очистка и фильтрация полученных строк.
    4. В транзакции удаление старых записей услуг и связанных с ними записей в products_services.
    5. Вставка новых данных услуг в таблицу services.
    6. Логирование этапов и возврат результата операции.
    """
    logger.info(f"Начало обновления Services для channel_id={channel_id}")

    conn = await asyncpg.connect(**settings.postgres_config)
    try:
        # Получение URL Google Sheet для канала
        channel_row = await conn.fetchrow(
            "SELECT url_googlesheet_data FROM channel WHERE id = $1", channel_id
        )
        if not channel_row or not channel_row["url_googlesheet_data"]:
            logger.error(f"URL GoogleSheet для канала {channel_id} не найден!")
            return False

        spreadsheet_url = channel_row["url_googlesheet_data"]

        # Создание ридера и получение всех строк из листа
        reader = await UniversalGoogleSheetReader.create(spreadsheet_url, sheet_name)
        rows = await reader.get_all_rows()

        # Очистка каждой строки для соответствия структуре БД
        cleaned_rows = [_clean_service_row(row, channel_id) for row in rows]

        # Фильтрация: оставляем только строки с непустым services_name
        rows_filtered = [row for row in cleaned_rows if row[1]]  # row[1] - service_name

        logger.info(
            f"Добавить в базу {len(rows_filtered)} строк Services для channel_id={channel_id}"
        )

        async with conn.transaction():
            # Получение id всех сервисов для удаления связанных записей
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

            # Удаление старых сервисов для канала
            result = await conn.execute(
                "DELETE FROM services WHERE channel_id = $1", channel_id
            )
            deleted_count = int(result.split()[-1])
            logger.info(
                f"Удалено из базы {deleted_count} строк Services для channel_id={channel_id}"
            )

            # Вставка новых сервисов
            if rows_filtered:
                await conn.executemany(
                    """
                    INSERT INTO services (
                        channel_id, services_name, services_full_name, description, indications,
                        contraindications, pre_session_instructions, indications_key,
                        contraindications_key, mult_score_boosting, body_parts
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    """,
                    rows_filtered,
                )

        logger.info(
            f"Добавлено в базу {len(rows_filtered)} строк Services для channel_id={channel_id}"
        )
        return True

    except Exception as e:
        logger.error(f"Ошибка обновления Services для channel_id={channel_id}: {e}")
        return False

    finally:
        await conn.close()


def _clean_service_row(row: dict, channel_id: int) -> tuple:
    """Обработка строк Google Sheets.

    Обрабатывает и очищает данные строки из Google Sheets для вставки в таблицу services.
    Очищает и формирует необходимые поля, убирает переносы строк из некоторых ключей.

    :param row: исходная строка из Google Sheets в виде словаря
    :param channel_id: идентификатор канала
    :return: кортеж с подготовленными значениями для вставки в БД
    """
    service_name = row.get("service", "").strip()
    body_parts = (
        (row.get("body_parts", "") or "").replace("\n", " ").replace("\r", " ").strip()
    )

    return (
        channel_id,
        service_name,
        f"{service_name} - {body_parts}",
        row.get("description"),
        row.get("indications"),
        row.get("contraindications"),
        row.get("pre_session_instructions"),
        (row.get("indications_key") or "").replace("\n", " ").replace("\r", " "),
        (row.get("contraindications_key") or "").replace("\n", " ").replace("\r", " "),
        row.get("mult_score_boosting"),
        body_parts,
    )


if __name__ == "__main__":
    """Тест запуска функции обновления services для канала с id=1."""
    result = asyncio.run(update_services_from_sheet(1))


# Запуск для проверки
# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update_postgres_qdrant.update_services_from_sheet
