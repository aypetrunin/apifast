import logging

# fastapi_app/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    langgraph_url: str = "http://172.17.0.1:8123"
    cors_origins: list[str] = ["*"]

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()


# -------------------- Logging --------------------
# Настройка логирования для вывода сообщений в консоль
logging.basicConfig(
    level=logging.INFO,  # минимальный уровень логирования INFO
    format="%(asctime)s [%(levelname)s] %(message)s",  # формат: время [уровень] сообщение
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)  # создаём логгер для текущего модуля
