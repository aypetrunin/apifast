"""Модуль обновления таблицы faq в postgres из GoogleSheet."""

import asyncio
import asyncpg

from .google_sheet_reader import UniversalGoogleSheetReader
from ..common import logger
from ..settings import settings


async def update_faq_from_sheet(channel_id: int, sheet_name: str = "faq") -> bool:
    """Асинхронная функция обновления FAQ из Google Sheets для указанного канала.

    Процесс работы:
    1. Получает из базы данных URL Google Sheets, связанный с channel_id.
    2. Создает объект чтения листа Google Sheets по sheet_name.
    3. Получает и валидирует все строки с вопросами и ответами.
    4. В рамках транзакции удаляет старые записи FAQ из базы для данного канала.
    5. Вставляет отфильтрованные новые записи в таблицу FAQ.
    6. Логгирует этапы и возвращает True в случае успеха или False при ошибке.
    """
    logger.info(f"Начало обновления FAQ для channel_id={channel_id}")
    conn = await asyncpg.connect(**settings.postgres_config)  # Подключение к БД PostgreSQL

    try:
        # Получение URL таблицы Google Sheets для данного канала
        spreadsheet_url = await _fetch_spreadsheet_url(conn, channel_id)

        # Создание ридера для чтения данных с указанного листа
        reader = await UniversalGoogleSheetReader.create(spreadsheet_url, sheet_name)

        # Получение всех строк с данными FAQ из Google Sheets
        faqs = await reader.get_all_rows()

        # Валидация и фильтрация полученных данных (убираются пустые/некорректные записи)
        faqs_filtered = _filter_valid_faqs(faqs)
        logger.info(
            f"Добавить в базу {len(faqs_filtered)} строк FAQ для channel_id={channel_id}"
        )

        # Использование транзакции для атомарного удаления старых и вставки новых записей
        async with conn.transaction():
            # Удаление старых FAQ из базы для данного канала
            deleted_count = await _delete_existing_faq(conn, channel_id)
            logger.info(
                f"Удалено из базы {deleted_count} строк FAQ для channel_id={channel_id}"
            )

            # Формирование кортежей значений для вставки в таблицу FAQ
            insert_tuples = _build_insert_tuples(faqs_filtered, channel_id)

            # Если есть новые записи, выполняем пакетную вставку в базу
            if insert_tuples:
                await conn.executemany(
                    "INSERT INTO faq (topic, question, answer, channel_id) VALUES ($1, $2, $3, $4)",
                    insert_tuples,
                )

        logger.info(
            f"Добавлено в базу {len(insert_tuples)} строк FAQ для channel_id={channel_id}"
        )
        return True

    except Exception as e:
        # Логгирование ошибки в случае неудачи обновления
        logger.error(f"Ошибка обновления FAQ для channel_id={channel_id}: {e}")
        return False

    finally:
        # Закрытие соединения с базой данных
        await conn.close()


async def _fetch_spreadsheet_url(conn, channel_id: int) -> str:
    """Асинхронное получение URL Google Sheets из базы по channel_id.

    Выполняет SQL-запрос для извлечения URL из таблицы channel.
    Если URL отсутствует, выбрасывает исключение с сообщением.
    """
    channel_row = await conn.fetchrow(
        "SELECT url_googlesheet_data FROM channel WHERE id = $1", channel_id
    )
    if not channel_row or not channel_row["url_googlesheet_data"]:
        logger.error(f"URL GoogleSheet для канала {channel_id} не найден!")
        raise ValueError("No GoogleSheet URL found")
    return channel_row["url_googlesheet_data"]


def _filter_valid_faqs(faqs: list[dict]) -> list[dict]:
    """Валидация и фильтрация списка FAQ.

    Каждая строка FAQ должна содержать непустые строковые значения в полях 'question' и 'answer'.
    Возвращает список только корректных записей.
    """

    def validate_faq_row(faq: dict) -> bool:
        question = faq.get("question", "")
        answer = faq.get("answer", "")
        return (
            isinstance(question, str)
            and question.strip()
            and isinstance(answer, str)
            and answer.strip()
        )

    return [faq for faq in faqs if validate_faq_row(faq)]


async def _delete_existing_faq(conn, channel_id: int) -> int:
    """Асинхронное удаление всех предыдущих FAQ из базы для заданного канала.

    Выполняется SQL-команда DELETE, возвращается количество удаленных записей.
    """
    result = await conn.execute("DELETE FROM faq WHERE channel_id = $1", channel_id)
    deleted_count = int(
        result.split()[-1]
    )  # Извлечение числа удаленных строк из результата
    return deleted_count


def _build_insert_tuples(faqs_filtered: list[dict], channel_id: int) -> list[tuple]:
    """Формирование списка кортежей значений для вставки в таблицу FAQ.

    Каждый кортеж содержит значения (topic, question, answer, channel_id).
    Если 'topic' отсутствует, используется пустая строка.
    """
    return [
        (faq.get("topic", ""), faq["question"], faq["answer"], channel_id)
        for faq in faqs_filtered
    ]


if __name__ == "__main__":
    """Тест запуска функции обновления FAQ для канала с id=1."""
    result = asyncio.run(update_faq_from_sheet(1))
    logger.info(f"Результат обновления: {result}")


# Запуск для проверки
# cd /home/copilot_superuser/petrunin/zena/apifast
# uv run python -m src.update_postgres_qdrant.update_faq_from_sheet
