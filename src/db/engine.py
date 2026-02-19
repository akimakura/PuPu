"""
Настройки работы с БД.
"""

from datetime import datetime
from ssl import SSLContext
from typing import Any, Optional
from urllib.parse import quote_plus

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import AsyncAdaptedQueuePool, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from src.config import settings
from src.models.database import (
    Connection as ConnectionModel,
    ConnetionTypeEnum,
    Database as DatabaseModel,
    DatabaseTypeEnum,
    Port as PortModel,
    ProtocolTypeEnum,
)
from src.utils.cert import get_ssl_context_by_database, get_ssl_mode_by_database

SEMANTIC_LAYER_DB_KEY = "DB_URL"

logger = EPMPYLogger(__name__)


class SimpelAsyncSession(AsyncSession):
    async def __aexit__(self, type_: Any, value: Any, traceback: Any) -> None:
        await self.close()


class DatabaseConnector:
    _instances: dict[str, "DatabaseConnector"] = {}
    _engines: dict[str, list[AsyncEngine]] = {}
    _async_session_makers: dict[str, list[async_sessionmaker[SimpelAsyncSession]]] = {}

    def __new__(
        cls,
        database: Optional[DatabaseModel] = None,
        is_semantic: bool = False,
    ) -> "DatabaseConnector":
        """Создание экземпляра класса и возврат, если уже создан для данного URL."""

        configured_db_url = DatabaseConnector.__get_key_from_db_urls(cls._get_configured_db_url(database, is_semantic))
        if configured_db_url in cls._instances:
            instance = cls._instances[configured_db_url]
            logger.debug("Got Database instance")
        else:
            instance = super(DatabaseConnector, cls).__new__(cls)
            cls._instances[configured_db_url] = instance
            logger.debug("Created Database instance")

        return instance

    @staticmethod
    def __get_key_from_db_urls(urls: dict[int, tuple[str, Optional[SSLContext]]]) -> str:
        url = ""
        for _, url_obj in urls.items():
            url += url_obj[0]
        return url

    def __init__(
        self,
        database: Optional[DatabaseModel] = None,
        is_semantic: bool = False,
    ) -> None:
        """Инициализация экземпляра, если он еще не инициализирован."""
        if database is None:
            raise AttributeError("""(database, tenant) or db_key should not be NULL""")
        if not hasattr(self, "initialized"):  # Проверка, инициализирован ли экземпляр
            self.is_semantic = is_semantic
            self.SQLALCHEMY_DATABASE_URI = self._get_configured_db_url(database, is_semantic)
            self.initialized = True
            self.database_name = database.name
            self.database_db_name = database.db_name
            self.key_connection = DatabaseConnector.__get_key_from_db_urls(self.SQLALCHEMY_DATABASE_URI)
            self._engines[self.key_connection] = self._create_engines()
            self._async_session_makers[self.key_connection] = self._create_async_session_makers()
            self.last_primary_session_maker: Optional[async_sessionmaker[SimpelAsyncSession]] = None
            self.last_primary_engine: Optional[AsyncEngine] = None

    @staticmethod
    def _get_configured_db_url(
        database: Optional[DatabaseModel] = None,
        is_semantic: bool = False,
    ) -> dict[int, tuple[str, Optional[SSLContext]]]:
        """
        Из переданного url формирует ключ вида "HOST_PORT_DBNAME"
        Получение драйвера БД из сырого url и формирование db_url из settings.
        """
        if database is None:
            raise AttributeError("""(database, tenant) or db_key should not be NULL""")
        if not is_semantic:
            user_key = f"DB_{database.tenant_id}_{database.name}_USER".upper()
            user_pass = f"DB_{database.tenant_id}_{database.name}_PASSWORD".upper()
        else:
            user_key = "DB_USER"
            user_pass = "DB_PASS"
        try:
            username = getattr(settings, user_key)
            password = getattr(settings, user_pass).replace("@", quote_plus("@"))
        except KeyError:
            raise KeyError(f"""Configuration key "{user_key}" or "{user_pass}" not found in settings.""")
        host = None
        port = None
        num_connection = 0
        last_ssl = "disable"
        last_context = None
        urls = {}
        for connection in database.connections:
            if connection.type != ConnetionTypeEnum.DATAGATE:
                ssl = get_ssl_mode_by_database(database, is_semantic, str(num_connection))
                if not ssl:
                    ssl = last_ssl
                last_ssl = ssl
                ssl_context = get_ssl_context_by_database(database, is_semantic, str(num_connection))
                if not ssl_context:
                    ssl_context = last_context
                last_context = ssl_context
                host = connection.host
                port = connection.ports[0].port
                urls[num_connection] = (
                    f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database.db_name}?ssl={ssl}",
                    ssl_context,
                )
                num_connection += 1
        if not host or not port:
            raise ValueError("Postgres host or port is not assigned")
        return urls

    def _create_engines(self) -> list[AsyncEngine]:
        """Создание SQLAlchemy engine для подключения к базе данных."""
        engines = []
        for _, node_url in self.SQLALCHEMY_DATABASE_URI.items():
            ssl_args = {"ssl": node_url[1]} if node_url[1] else {}
            engines.append(
                create_async_engine(
                    node_url[0],
                    echo=settings.DB_ECHO,
                    poolclass=AsyncAdaptedQueuePool,
                    connect_args=ssl_args,
                    pool_pre_ping=settings.SQLALCHEMY_POOL_PRE_PING,
                    pool_recycle=settings.SQLALCHEMY_POOL_RECYCLE,
                    pool_size=settings.SQLALCHEMY_POOL_SIZE,
                    max_overflow=settings.SQLALCHEMY_MAX_OVERFLOW,
                    pool_timeout=settings.SQLAlCHEMY_POOL_TIMEOUT,
                )
            )
        return engines

    def _create_async_session_makers(self) -> list[async_sessionmaker[SimpelAsyncSession]]:
        """Создание SQLAlchemy engine для подключения к базе данных."""
        engines = self.get_engines()
        async_sessionmakers = []
        for engine in engines:
            async_sessionmakers.append(
                async_sessionmaker(
                    bind=engine,
                    autocommit=False,
                    autoflush=False,
                    expire_on_commit=False,
                    class_=SimpelAsyncSession,
                )
            )
        return async_sessionmakers

    def get_engines(self) -> list[AsyncEngine]:
        """
        Получение соединений с engine
        """
        return self._engines[self.key_connection]

    def get_async_session_makers(self) -> list[async_sessionmaker[SimpelAsyncSession]]:
        """Инициализация создателя сессий"""
        return self._async_session_makers[self.key_connection]

    async def get_not_pg_is_in_recovery(self) -> tuple[AsyncEngine, async_sessionmaker[SimpelAsyncSession]]:
        last_valid_session_maker = None
        last_valid_engine = None
        engines = self.get_engines()
        async_session_makers: list[async_sessionmaker] = self.get_async_session_makers()
        for num_connection, async_session_maker in enumerate(async_session_makers):
            try:
                async with async_session_maker() as session:
                    engine = engines[num_connection]
                    is_in_recovery_record = await session.execute(text("SELECT pg_is_in_recovery();"))
                    is_in_recovery = is_in_recovery_record.fetchall()[0][0]
                    last_valid_session_maker = async_session_maker
                    last_valid_engine = engine
            except Exception:
                logger.exception("Connection error to the node %s", num_connection)
                continue
            if not is_in_recovery:
                logger.debug("Database node %s is primary", num_connection)
                self.last_primary_session_maker = async_session_maker
                self.last_primary_engine = engine
                return engine, async_session_maker
        logger.warning("No node was found that is not in pg_is_in_recovery mode.")
        if last_valid_session_maker and last_valid_engine:
            return last_valid_engine, last_valid_session_maker
        db_label = self.database_db_name or self.database_name or "unknown"
        raise Exception(f"There is no available connection nodes for database '{db_label}'.")


def create_semantic_database_model() -> DatabaseModel:
    connections = []
    for node_num in range(settings.COUNT_DATABASE_NODES):
        node_label = "" if node_num == 0 else str(node_num)
        db_host = getattr(settings, f"DB{node_label}_HOST", None)
        db_port = getattr(settings, f"DB{node_label}_PORT", None)
        if not db_host or not db_port:
            continue
        connections.append(
            ConnectionModel(
                host=db_host,
                type=ConnetionTypeEnum.NODE,
                ports=[
                    PortModel(
                        port=db_port,
                        protocol=ProtocolTypeEnum.POSTGRESQL_V3,
                        sql_dialect=DatabaseTypeEnum.POSTGRESQL,
                        secured=False,
                    )
                ],
            )
        )
    return DatabaseModel(
        user="system",
        timestamp=datetime.now(),
        version=1,
        name="semantic_layer",
        tenant_id="",
        db_name=settings.DB_NAME,
        type=DatabaseTypeEnum.POSTGRESQL,
        connections=connections,
    )


database_connector = DatabaseConnector(
    database=create_semantic_database_model(),
    is_semantic=True,
)
