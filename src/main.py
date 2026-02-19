"""
Модуль запуска WEB-сервера.
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI
from py_common_lib.logger import EPMPYLogger
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
from src.api.v1 import router as v1_router
from src.cache.middleware import ReadOnlyCacheMiddleware
from src.config import settings
from src.errror_handlers import setup_error_handlers
from src.metrics.middleware import PrometheusBusinessMetricsMiddleware
from src.on_startup import setup_fastapi_cache, setup_logging_configs, start_kafka_producer, stop_kafka_producer

logger: EPMPYLogger = EPMPYLogger(__name__)


API_PREFIX_URL = "/api"
API_GATEWAY_PREFIX = "/web-api/epmp/semlayer/api"


def setup_middleware(app: FastAPI) -> None:
    """
    Настройка Middleware.

    Args:
        app (FastAPI): экземпляр FastAPI
    """
    app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=True)
    app.add_middleware(ReadOnlyCacheMiddleware)
    app.add_middleware(PrometheusBusinessMetricsMiddleware, filter_unhandled_paths=True)
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
            expose_headers=["x-b3-traceid", "X-B3-TRACEID"],
        )


def setup_routers(app: FastAPI) -> None:
    """
    Инициализация роутера.
    """
    app.include_router(router=internal_router, prefix="/api/internal", tags=["internal"])
    app.include_router(router=internal_router, prefix=API_GATEWAY_PREFIX + "/internal", tags=["web-api/internal"])
    app.include_router(router=v0_router, prefix=API_PREFIX_URL, tags=["v0"])
    app.include_router(router=v0_router, prefix=API_GATEWAY_PREFIX, tags=["web-api/v0"])
    app.include_router(router=v1_router, prefix=API_PREFIX_URL, tags=["v1"])
    app.include_router(router=v1_router, prefix=API_GATEWAY_PREFIX, tags=["web-api/v1"])
    # Роут с метриками контейнера
    app.add_route("/metrics", metrics_endpoint)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Управление запуском/завершением.
    """
    logger.info("Run start on pod %s", settings.HOSTNAME)
    await setup_logging_configs()
    await setup_fastapi_cache()
    await start_kafka_producer()
    logger.info("Start successfully pod %s", settings.HOSTNAME)
    yield
    await stop_kafka_producer()
    logger.info("Stop pod %s", settings.HOSTNAME)


def create_app(*args: Any, **kwargs: Any) -> FastAPI:
    """
    Метод создания FastAPI web-сервера.
    """
    app = FastAPI(docs_url="/swagger", lifespan=lifespan, version="1.0.7v19")
    setup_error_handlers(app)
    setup_middleware(app)
    setup_routers(app)
    return app


# TODO: Поменять везде ошибки на русский язык (Предложил Тимур Хамзин)
# TODO: Расширить все докстринги. https://jira.sberbank.ru/browse/EPMPY-609  (Предложил Александр Якушкин)
