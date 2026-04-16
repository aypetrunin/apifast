"""Модуль запуска FastAPI-приложения apifast."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.settings import settings
from src.zena_logging import setup_logging, get_logger
from src.routes.agent import router as agent_router
from src.routes.reminders import reminders_router
from src.routes.health import router as health_router
from src.routes.update_faq import router as update_faq_router
from src.routes.update_promo import router as update_promo_router
from src.routes.update_services import router as update_services_router
from src.routes.update_products import router as update_product


setup_logging()
logger = get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Жизненный цикл приложения."""
    logger.info("app.started")
    yield
    logger.info("app.stopped")


app = FastAPI(title="FastAPI ↔ LangGraph", debug=True, lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Обработчик ошибок валидации."""
    body = await request.body()
    logger.error("validation.error", errors=exc.errors(), body=body.decode())
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(agent_router)
app.include_router(reminders_router)
app.include_router(update_faq_router)
app.include_router(update_promo_router)
app.include_router(update_services_router)
app.include_router(update_product)
app.include_router(health_router)
