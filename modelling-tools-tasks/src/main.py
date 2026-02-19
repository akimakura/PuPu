"""
Модуль запуска WEB-сервера.
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI
from py_common_lib.logger import get_logger
from py_common_lib.metrics import PrometheusMiddleware, metrics_endpoint
from py_common_lib.starlette_context_plugins import (
    AuthorizationPlugin,
    ClientHostPlugin,
    EndpointMethodPlugin,
    EndpointPathPlugin,
    Xb3FlagsPlugin,
    Xb3ParentSpanIdPlugin,
    Xb3SampledPlugin,
    Xb3SpanIdPlugin,
    Xb3TraceIdPlugin,
)
from starlette.middleware.cors import CORSMiddleware
from starlette_context.middleware import RawContextMiddleware

from src.api.internal.router import router as internal_router
from src.api.v0 import router as v0_router
from src.broker import broker
from src.config import settings
from src.on_startup import setup_logging_configs

logger = get_logger(__name__)


def setup_middleware(app: FastAPI) -> None:
    """
    Настройка Middleware.
    """
    app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=True)
    app.add_middleware(
        RawContextMiddleware,
        plugins=[
            AuthorizationPlugin(),
            ClientHostPlugin(),
            EndpointPathPlugin(),
            EndpointMethodPlugin(),
            Xb3ParentSpanIdPlugin(),
            Xb3SpanIdPlugin(),
            Xb3TraceIdPlugin(),
            Xb3SampledPlugin(),
            Xb3FlagsPlugin(),
        ],
    )
    if settings.BACKEND_CORS_ORIGINS:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )


def setup_routers(app: FastAPI) -> None:
    """
    Инициализация роутера.
    """
    app.include_router(router=internal_router, prefix="/api/internal", tags=["internal"])
    app.include_router(router=v0_router, prefix="/api", tags=["v0"])

    # Роут с метриками контейнера
    app.add_route("/metrics", metrics_endpoint)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Управление запуском/завершением.
    """
    if not broker.is_worker_process:
        await broker.startup()
        logger.info("Run start on pod %s", settings.HOSTNAME)
        await setup_logging_configs()
        logger.info("Start successfully pod %s", settings.HOSTNAME)
    yield
    if not broker.is_worker_process:
        await broker.shutdown()
        logger.info("Stop pod %s", settings.HOSTNAME)


def create_app(*args: Any, **kwargs: Any) -> FastAPI:
    """
    Метод создания FastAPI web-сервера.
    """
    app = FastAPI(docs_url="/swagger", lifespan=lifespan)
    setup_middleware(app)
    setup_routers(app)
    return app
