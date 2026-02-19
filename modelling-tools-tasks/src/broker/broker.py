from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import taskiq_fastapi
from py_common_lib.logger import EPMPYLogger
from taskiq import TaskiqEvents, TaskiqState
from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

from src.config import settings
from src.on_startup import setup_logging_configs

logger = EPMPYLogger(__name__)
result_backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=settings.REDIS_URL,
    result_ex_time=settings.RESULT_EX_TIME,
)
broker = RedisStreamBroker(url=settings.REDIS_URL).with_result_backend(result_backend)
taskiq_fastapi.init(broker, "src.main:create_app")


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def on_worker_startup(state: TaskiqState) -> None:
    await setup_logging_configs()
    logger.info("Worker startup complete")


@asynccontextmanager
async def with_task_broker() -> AsyncGenerator[None, Any]:
    await broker.startup()
    try:
        logger.info("Task broker connection startup")
        yield
    finally:
        await broker.shutdown()
        logger.info("Task broker conneciton shutdown")
