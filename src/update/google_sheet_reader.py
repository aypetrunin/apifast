"""Модуль реализует универсальный класс чтения из GoogleSheet по URL."""

# google_sheet_reader.py

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Type

import gspread

from ..common import retry_async  # type: ignore
from ..zena_logging import get_logger  # type: ignore

logger = get_logger()

# 🔐 кеш временного файла, чтобы не плодить файлы при retry
_TMP_SA_FILE: str | None = None


BASE_DIR = Path(__file__).resolve().parents[3]   # /app
SERVICE_ACCOUNT_FILE = str(BASE_DIR / "deploy" / "aiucopilot-d6773dc31cb0.json")

def get_service_account_file() -> str:
    """
    Возвращает путь к json сервисного аккаунта.

    Приоритет:
    1) SERVICE_ACCOUNT_FILE — если передан путь и файл существует
    2) GOOGLE_SA_JSON — строкой (из env / env_file) → пишем во временный файл
    """
    global _TMP_SA_FILE

    # 1️⃣ Явно переданный путь
    path = os.getenv("SERVICE_ACCOUNT_FILE")
    if path and Path(path).exists():
        return path

    # 2️⃣ Уже созданный временный файл (при retry)
    if _TMP_SA_FILE and Path(_TMP_SA_FILE).exists():
        return _TMP_SA_FILE

    # 3️⃣ JSON из env
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if not sa_json:
        raise RuntimeError(
            "Missing Google credentials: "
            "set GOOGLE_SA_JSON or SERVICE_ACCOUNT_FILE"
        )

    # Проверяем, что JSON валидный (часто ловит ошибки env_file)
    json.loads(sa_json)

    # Пишем во временный файл
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".json",
    )
    tmp.write(sa_json)
    tmp.flush()
    tmp.close()

    _TMP_SA_FILE = tmp.name
    return _TMP_SA_FILE


class UniversalGoogleSheetReader:
    """Класс универсального чтения из Google Sheets."""

    def __init__(
        self,
        spreadsheet_url: str,
        sheet_name: str,
        service_account_file: str | None = None,
    ) -> None:
        """
        :param spreadsheet_url: URL Google таблицы
        :param sheet_name: имя листа
        :param service_account_file: путь к json (опционально)
            Если None — будет взят из env (SERVICE_ACCOUNT_FILE или GOOGLE_SA_JSON)
        """
        self.spreadsheet_url = spreadsheet_url
        self.sheet_name = sheet_name
        self.service_account_file = service_account_file

        self.gc = None
        self.sh = None
        self.ws = None
        self.headers: list[str] = []

    @retry_async()
    async def _init_google_client(self) -> None:
        """Инициализация Google Sheets клиента с retry."""
        await self._real_init()

    async def _real_init(self) -> None:
        """
        - аутентификация сервисным аккаунтом
        - открытие таблицы
        - получение листа
        - чтение заголовков
        """
        try:
            # 🔑 гарантируем существование json-файла
            if not self.service_account_file:
                self.service_account_file = get_service_account_file()

            self.gc = gspread.service_account(self.service_account_file)
            self.sh = self.gc.open_by_url(self.spreadsheet_url)
            self.ws = self.sh.worksheet(self.sheet_name)
            self.headers = self.ws.row_values(1)

        except gspread.exceptions.APIError as api_err:
            logger.error(
                "google_sheets.error",
                stage="initialization",
                error=str(api_err),
            )
            raise
        except gspread.exceptions.GSpreadException as gs_err:
            logger.error(
                "google_sheets.error",
                stage="initialization",
                error=str(gs_err),
            )
            raise
        except Exception as e:
            logger.error(
                "google_sheets.error",
                stage="initialization",
                error=str(e),
            )
            raise

    async def _get_all_rows_async(self) -> list[dict[str, Any]]:
        """Асинхронное чтение всех строк."""
        try:
            rows = await asyncio.to_thread(self.ws.get_all_values)
            return [dict(zip(self.headers, row)) for row in rows[1:]]
        except gspread.exceptions.APIError as api_err:
            logger.error("google_sheets.error", stage="reading", error=str(api_err))
            return []
        except gspread.exceptions.GSpreadException as gs_err:
            logger.error("google_sheets.error", stage="reading", error=str(gs_err))
            return []
        except Exception as e:
            logger.error("google_sheets.error", stage="reading", error=str(e))
            return []

    @retry_async()
    async def get_all_rows(self) -> list[dict[str, Any]]:
        """Публичный метод получения всех строк."""
        return await self._get_all_rows_async()

    @classmethod
    async def create(
        cls: Type["UniversalGoogleSheetReader"],
        spreadsheet_url: str,
        sheet_name: str,
        service_account_file: str | None = None,
    ) -> "UniversalGoogleSheetReader":
        """
        Асинхронный фабричный метод.
        """
        self = cls(spreadsheet_url, sheet_name, service_account_file)
        await self._init_google_client()
        return self

