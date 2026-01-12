"""Модуль реализует универсальный класс чтения из GoogleSheet по URL."""

import asyncio
import os
from typing import Any, Type
from pathlib import Path

import gspread
import gspread.exceptions

from ..common import logger, retry_async  # type: ignore

# SERVICE_ACCOUNT_FILE = os.path.join(
#     os.path.dirname(__file__), "aiucopilot-d6773dc31cb0.json"
# )
BASE_DIR = Path(__file__).resolve().parents[1]  # корень проекта
SERVICE_ACCOUNT_FILE = BASE_DIR / "deploy" / "aiucopilot-d6773dc31cb0.json"


class UniversalGoogleSheetReader:
    """Класс универсального чтения из GoogleSheet."""

    def __init__(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        service_account_file: str = SERVICE_ACCOUNT_FILE,
    ) -> None:
        """Конструктор класса.

        Сохраняет параметры подключения к Google Sheet:
        - spreadsheet_url: URL Google таблицы
        - sheet_name: имя листа в таблице
        - service_account_file: путь к файлу сервисного аккаунта для доступа к API
        Само подключение и инициализация Google клиента делаются отдельно асинхронно.
        """
        self.spreadsheet_url = spreadsheet_url
        self.sheet_name = sheet_name
        self.service_account_file = service_account_file

    @retry_async()
    async def _init_google_client(self) -> None:
        """Инициализации Google Sheets API клиента.

        Асинхронный метод инициализации Google Sheets API клиента
        с использованием retry_request для повторных попыток в случае ошибок.
        """
        await self._real_init()

    async def _real_init(self) -> None:
        """Внутренняя асинхронная инициализация.

        - аутентификация сервисным аккаунтом,
        - открытие таблицы по URL,
        - загрузка нужного листа,
        - считывание заголовков (первой строки).
        """
        try:
            self.gc = gspread.service_account(self.service_account_file)
            self.sh = self.gc.open_by_url(self.spreadsheet_url)
            self.ws = self.sh.worksheet(self.sheet_name)
            self.headers = self.ws.row_values(1)
        except gspread.exceptions.APIError as api_err:
            logger.error(f"Google Sheets API Error during initialization: {api_err}")
            raise
        except gspread.exceptions.GSpreadException as gs_err:
            logger.error(f"Gspread general error: {gs_err}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Google Sheets initialization: {e}")
            raise

    async def _get_all_rows_async(self) -> list[dict[str, Any]]:
        """Асинхронный метод считывания всех строк с листа Google Sheets.

        Использует asyncio.to_thread для выполнения синхронного метода get_all_values
        в отдельном потоке, чтобы не блокировать event loop.
        Возвращает список словарей, где каждый словарь — строка с ключами из заголовков.
        При ошибке логирует и возвращает пустой список.
        """
        try:
            rows = await asyncio.to_thread(self.ws.get_all_values)
            return [dict(zip(self.headers, row)) for row in rows[1:]]
        except gspread.exceptions.APIError as api_err:
            logger.error(f"Google Sheets API Error во время чтения данных: {api_err}")
            return []
        except gspread.exceptions.GSpreadException as gs_err:
            logger.error(f"Gspread общая ошибка во время чтения данных: {gs_err}")
            return []
        except Exception as e:
            logger.error(f"Неожиданная ошибка при чтении данных из  Google Sheet: {e}")
            return []

    @retry_async()
    async def get_all_rows(self) -> list[dict[str, Any]]:
        """Асинхронный метод для получения всех строк.

        Запускает асинхронный метод _get_all_rows_async с retry_request
        через asyncio.run, чтобы из синхронного кода получить результат.
        """
        return await self._get_all_rows_async()

    @classmethod
    async def create(
        cls: Type["UniversalGoogleSheetReader"],
        spreadsheet_url: str,
        sheet_name: str,
        service_account_file: str = SERVICE_ACCOUNT_FILE,
    ) -> "UniversalGoogleSheetReader":
        """Асинхронный фабричный метод для создания и полной асинхронной инициализации экземпляра.

        Позволяет создавая объект сразу получить готовый к работе экземпляр.
        """
        self = cls(spreadsheet_url, sheet_name, service_account_file)
        await self._init_google_client()
        return self
