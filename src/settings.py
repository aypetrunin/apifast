"""Модуль определения переменных проекта."""

import os
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


def is_docker() -> bool:
    """Функция определят запущен код в докерк или нет."""
    return str(os.getenv("IS_DOCKER", "0")).lower() in ("1", "true", "yes")


class Settings(BaseSettings):
    """Опреление системных переменных."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    cors_origins: list[str] = ["*"]

    langgraph_url_docker: str = "http://langgraph-api:8000"
    langgraph_url_no_docker: str = "http://localhost:2024"

    @property
    def langgraph_url(self) -> str:
        """Определение свойства."""
        return (
            self.langgraph_url_docker if is_docker() else self.langgraph_url_no_docker
        )

    qdrant_url: str
    qdrant_timeout: int
    qdrant_collection_faq: str
    qdrant_collection_services: str
    qdrant_collection_products: str
    qdrant_collection_temp: str

    openai_api_key: str
    openai_proxy_url: str
    openai_timeout: int

    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int

    @property
    def postgres_config(self) -> dict[str, Any]:
        """Определение свойства."""
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "database": self.postgres_db,
            "host": self.postgres_host,
            "port": self.postgres_port,
        }


settings = Settings()
