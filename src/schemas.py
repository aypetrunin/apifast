"""Модель параметров передаваемых агенту."""

from typing import Any, Dict

from pydantic import BaseModel, Field


class AgentRunParams(BaseModel):
    """Плоские поля из входного JSON."""

    user_id: int
    message: str
    reply_to_history_id: int
    user_companychat: int | str
    access_token: str

    # опциональные, встречаются не всегда
    group_id: int | str | None = None
    platform: str | None = None
    keyboard: Dict[str, Any] | None = None

    # служебные поля для вашего пайплайна
    assistant_id: str | None = Field(default="agent_zena")
    config: Dict[str, Any] | None = None
    metadata: Dict[str, Any] | None = None
