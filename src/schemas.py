# fastapi_app/schemas.py
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional


class AgentRunParams(BaseModel):
    # плоские поля из входного JSON
    user_id: int
    message: str
    reply_to_history_id: int
    user_companychat: int | str
    access_token: str

    # опциональные, встречаются не всегда
    group_id: Optional[int | str] = None
    platform: Optional[str] = None
    keyboard: Optional[Dict[str, Any]] = None

    # служебные поля для вашего пайплайна
    assistant_id: Optional[str] = Field(default="agent_zena")
    config: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
