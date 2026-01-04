"""Модуль определения переменных проекта."""

import os
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


print(os.getcwd())  # Должна совпадать с местом, куда скопировали .env

def is_docker() -> bool:
    """Функция определят запущен код в докерк или нет."""
    return str(os.getenv("IS_DOCKER", "0")).lower() in ("1", "true", "yes")


class Settings(BaseSettings):
    """Опреление системных переменных."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
        env_file_encoding="utf-8",
    )

    cors_origins: list[str] = ["*"]

    langgraph_url_docker: str = "http://langgraph-api:8000"
    langgraph_url_no_docker: str = "http://localhost:2024"

    @property
    def langgraph_url(self) -> str:
        """Определение свойства."""
        return (
            self.langgraph_url_docker if is_docker() else self.langgraph_url_no_docker
        )

    qdrant_url: str = Field(env='QDRANT_URL')
    qdrant_timeout: int = Field(env='QDRANT_TIMEOUT')
    qdrant_collection_faq: str = Field(env='QDRANT_COLLECTION_FAQ')
    qdrant_collection_services: str = Field(env='QDRANT_COLLECTION_SERVICES')
    qdrant_collection_products: str = Field(env='QDRANT_COLLECTION_PRODUCTS')
    qdrant_collection_temp: str = Field(env='QDRANT_COLLECTION_TEMP')

    openai_api_key: str = Field(env='OPENAI_API_KEY')
    openai_proxy_url: str = Field(env='OPENAI_PROXY_URL')
    openai_timeout: int = Field(env='OPENAI_TIMEOUT')

    postgres_user: str = Field(env='POSTGRES_USER')
    postgres_password: str = Field(env='POSTGRES_PASSWORD')
    postgres_db: str = Field(env='POSTGRES_DB')
    postgres_host: str = Field(env='POSTGRES_HOST')
    postgres_port: int = Field(env='POSTGRES_PORT')

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