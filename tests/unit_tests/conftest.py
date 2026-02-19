import logging
from http import HTTPStatus
from typing import Any, AsyncGenerator, List, Optional, Tuple
from unittest.mock import MagicMock

import jwt
import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from py_common_lib.permissions import PermissionChecker, User
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from src.cache import FastAPICache
from src.cache.backends.inmemory import InMemoryBackend
from src.config import settings
from src.db import metadata
from src.db.engine import SimpelAsyncSession, database_connector
from src.main import create_app

MOCK_CONFIG_TYPE = List[Tuple[str, List]]
MOCK_TOKEN = jwt.encode(
    {"sub": "1234567890", "name": "John Doe", "iat": 1516239022}, algorithm="HS256", key=settings.TEST_TOKEN_KEY
)
logger = logging.getLogger(__name__)


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(element: JSONB, compiler: Any, **kwargs: Any) -> str:
    return "JSON"


@pytest.fixture(autouse=True)
def set_default_settings() -> None:
    settings.ENABLE_GENERATE_OBJECTS = False
    settings.ENABLE_SWITCH_HOST = False
    return None


@pytest.fixture(autouse=True)
async def permissions(monkeypatch: pytest.MonkeyPatch) -> None:
    def call_mock(user: User) -> User:
        return User(username="", permissions=[])

    monkeypatch.setattr(PermissionChecker, "__call__", call_mock)
    return None


@pytest.fixture(autouse=True)
async def in_memory_cache() -> AsyncGenerator[None, Any]:
    in_memory_backend = InMemoryBackend()
    FastAPICache.init(backend=in_memory_backend, enable=True)
    yield None
    if FastAPICache._backend is not None:
        await FastAPICache._backend.clear()


@pytest.fixture(scope="session")
def connection_url() -> str:
    return "sqlite+aiosqlite:///:memory:"


@pytest.fixture
def engine(connection_url: str) -> AsyncEngine:
    return create_async_engine(connection_url, pool_pre_ping=True)


@pytest.fixture
async def connection(engine: AsyncEngine) -> AsyncConnection:
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.execute(text(f"ATTACH DATABASE ':memory:' AS {settings.DB_SCHEMA};"))
        await conn.run_sync(metadata.create_all)
    return await engine.connect()


@pytest.fixture
async def mocked_session(connection: AsyncConnection) -> AsyncGenerator[AsyncSession, None]:
    session: AsyncSession = async_sessionmaker(
        autocommit=False, autoflush=False, expire_on_commit=False, class_=SimpelAsyncSession
    )(bind=connection)
    yield session
    await session.close()
    await connection.close()


@pytest.fixture
def fastapi_app() -> FastAPI:
    return create_app()


@pytest.fixture
async def async_client(fastapi_app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app), base_url="http://test", headers={"Authorization": f"Bearer {MOCK_TOKEN}"}  # type: ignore
    ) as ac:
        yield ac


def mock_session_maker(
    monkeypatch: pytest.MonkeyPatch, session: AsyncSession, tenant_id: Optional[str] = None, check_tenant: bool = True
) -> None:
    async_session_maker_mock = MagicMock()
    async_session_maker_mock.return_value.__aenter__.return_value = session

    async def mock_session() -> Any:
        return MagicMock(), async_session_maker_mock

    monkeypatch.setattr(database_connector, "get_not_pg_is_in_recovery", mock_session)


def mock_method_by_http_exception(
    monkeypatch: pytest.MonkeyPatch, service: Any, method_name: str, status_code: HTTPStatus
) -> None:
    def mock_method(*args: Any, **kwargs: Any) -> Any:
        raise HTTPException(status_code=status_code)

    monkeypatch.setattr(service, method_name, mock_method)
