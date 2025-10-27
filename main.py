# fastapi_app/main.py
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.settings import settings
from src.routes.agent import router as agent_router
from src.routes.health import router as health_router
from src.routes.faq_update import router as faq_update


logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
logger.addHandler(_handler)

app = FastAPI(title="FastAPI ↔ LangGraph", debug=True)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    logger.error(f"422 validation: errors={exc.errors()} body={body!r}")
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(agent_router)
app.include_router(faq_update)
app.include_router(health_router)
