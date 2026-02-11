# schemas.py

"""Модель параметров передаваемых агенту."""

from typing import Any, ClassVar

from pydantic import BaseModel, model_validator

from .common import logger


class AgentRunParams(BaseModel):
    """Плоские поля из входного JSON."""

    user_id: int
    reply_to_history_id: int
    user_companychat: int | str
    access_token: str
    mcp_port: int

    group_id: int | str | None = None
    platform: str | None = None
    keyboard: dict[str, Any] | None = None

    message: str
    assistant_id: str | None = None
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None

    # ВАЖНО: ClassVar => это НЕ поле модели, Pydantic не будет ругаться
    agent: ClassVar[dict[str, list[int]]] = {
        "agent_zena_alisa": [5001, 15001],
        "agent_zena_sofia": [5002, 15002],
        "agent_zena_anisa": [5005, 15005],
        "agent_zena_annitta": [5006, 15006],
        "agent_zena_anastasia": [5007, 15007],
        "agent_zena_alena": [5020, 15020],
        "agent_zena_valentina": [5021, 15021],
        "agent_zena_marina": [5024, 15024],
        "agent_zena_egoistka": [5017, 15017],
    }

    @classmethod
    def get_agent_by_mcp_port(cls, mcp_port: int) -> str:
        for agent_name, ports in cls.agent.items():
            if mcp_port in ports:
                return agent_name
        raise ValueError(f"Agent not found for mcp_port={mcp_port}")

    @model_validator(mode="before")
    @classmethod
    def set_assistant_id_by_mcp_port(cls, values: Any):
        # values должен быть dict на стадии before
        if not isinstance(values, dict):
            return values

        logger.info(f"values={values}")

        mcp_port = values.get("mcp_port") or 5007
        logger.info(f"mcp_port={mcp_port}")

        assistant_id = cls.get_agent_by_mcp_port(mcp_port)

        values.update(
            {
                "context": {
                    "_user_id": values.get("user_id"),
                    "_reply_to_history_id": values.get("reply_to_history_id"),
                    "_user_companychat": values.get("user_companychat"),
                    "_access_token": values.get("access_token"),
                    "_group_id": values.get("group_id"),
                    "_platform": values.get("platform"),
                },
                "mcp_port": mcp_port,
                "assistant_id": assistant_id,
            }
        )

        logger.info(f"assistant_id={assistant_id}")
        return values
