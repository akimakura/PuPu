"""
Настройки работы с БД.
"""

from ssl import SSLContext
from typing import Any, Optional, Sequence
from urllib.parse import quote_plus

from py_common_lib.logger import EPMPYLogger
from py_common_lib.logger.audit_types import F9
from sqlalchemy import AsyncAdaptedQueuePool, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from src.config import settings
from src.db.utils import get_ip_address_by_dns_name
from src.integrations.modelling_tools_api.codegen import (
    Database as DatabaseModel,
)
from src.models.connection import ConnetionTypeEnum
from src.utils.cert import get_ssl_context_by_database, get_ssl_mode_by_database

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
        tenant_id: str,
        database: Optional[DatabaseModel] = None,
        is_semantic: bool = False,
    ) -> "DatabaseConnector":
        """Создание экземпляра класса и возврат, если уже создан для данного URL."""

        configured_db_url = DatabaseConnector.__get_key_from_db_urls(
            cls._get_configured_db_url(tenant_id, database, is_semantic)
        )
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
        tenant_id: str,
        database: Optional[DatabaseModel] = None,
        is_semantic: bool = False,
    ) -> None:
        """Инициализация экземпляра, если он еще не инициализирован."""
        if not hasattr(self, "initialized"):  # Проверка, инициализирован ли экземпляр
            self.is_semantic = is_semantic
            self.SQLALCHEMY_DATABASE_URI = self._get_configured_db_url(tenant_id, database, is_semantic)
            self.initialized = True
            self.key_connection = DatabaseConnector.__get_key_from_db_urls(self.SQLALCHEMY_DATABASE_URI)
            self._engines[self.key_connection] = self._create_engines()
            self._async_session_makers[self.key_connection] = self._create_async_session_makers()
            self.last_primary_session_maker: Optional[async_sessionmaker[SimpelAsyncSession]] = None
            self.last_primary_engine: Optional[AsyncEngine] = None

    @staticmethod
    def _get_configured_db_url(
        tenant_id: str,
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
            user_key = f"DB_{tenant_id}_{database.name}_USER".upper()
            user_pass = f"DB_{tenant_id}_{database.name}_PASSWORD".upper()
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
                ssl = get_ssl_mode_by_database(tenant_id, database, is_semantic, str(num_connection))
                if not ssl:
                    ssl = last_ssl
                last_ssl = ssl
                ssl_context = get_ssl_context_by_database(tenant_id, database, is_semantic, str(num_connection))
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
                    pool_pre_ping=True,
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
        async_session_makers: list[async_sessionmaker[SimpelAsyncSession]] = self.get_async_session_makers()
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
        raise Exception("There is no available connection nodes")


async def execute_raw_DDL(tenant_id: str, database: DatabaseModel, queries: str | list[str]) -> None:
    """
    Выполняет один или несколько запросов DDL (Data Definition Language).

    Примеры запросов DDL включают команды типа CREATE, ALTER TABLE, DROP TABLE и другие операции,
    влияющие на структуру базы данных.

    Args:
        tenant_id (str): Идентификатор арендатора/клиента системы.
        database (Database): Объект класса `Database`, содержащий параметры подключения к БД.
        queries (str | list[str]): Строка или список строк с SQL-запросами.

    Raises:
        OperationalError: В случае ошибки выполнения запроса (например, синтаксической ошибки).
        ConnectionRefusedError: Если подключение к серверу баз данных было отклонено.
        Exception: Любая другая непредвиденная ошибка во время выполнения запроса.
    """
    try:
        db = DatabaseConnector(tenant_id=tenant_id, database=database)
        _, async_session_maker = await db.get_not_pg_is_in_recovery()
        async with async_session_maker() as session:
            if isinstance(queries, list):
                for query in queries:
                    query = query.replace("`", '"')
                    logger.debug("""EXECUTE QUERY: "%s";""", query)
                    await session.execute(text(query))
            else:
                logger.debug("""EXECUTE QUERY: "%s";""", queries)
                await session.execute(text(queries))
            await session.commit()
            return None
    except ConnectionRefusedError as ext:
        ip_address = get_ip_address_by_dns_name(database.connections[0].host)
        logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
        raise ConnectionRefusedError(str(ext))


async def execute_raw_DQL_or_DML(
    tenant_id: str, database: DatabaseModel, query: str, params: Optional[dict[Any, Any]] = None
) -> Sequence[Any]:
    """
    Выполняет SQL-запросы, возвращающие набор строк (например SELECT или UPDATE с клаузулой RETURNING).

    Args:
        tenant_id (str): Уникальный идентификатор арендатора/клиента.
        database (Database): Объект базы данных, содержащий параметры подключения.
        query (str): Строка с SQL-запросом.
        params (Optional[dict[Any, Any]], optional): Параметры для подстановки в запрос (по умолчанию None).

    Returns:
        list[Sequence[Any]]: Список кортежей (строк), каждая из которых содержит значения столбцов результата.

    Raises:
        OperationalError: Если возникла ошибка работы с базой данных.
        ConnectionRefusedError: В случае отказа соединения с сервером БД.
        Exception: Любая другая необработанная ошибка выполнения запроса.
    """
    try:
        db = DatabaseConnector(tenant_id=tenant_id, database=database)
        _, async_session_maker = await db.get_not_pg_is_in_recovery()
        async with async_session_maker() as session:
            query = query.replace("`", '"')
            logger.debug("""EXECUTE QUERY: "%s"; with params=%s""", query, params)
            result_gp = await session.execute(text(query), params)
            return result_gp.tuples().all()

    except ConnectionRefusedError as exc:
        ip_address = get_ip_address_by_dns_name(database.connections[0].host)
        logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
        raise ConnectionRefusedError(str(exc))
    except ProgrammingError as exc:
        if "UndefinedTableError" in str(exc):
            logger.warning("Unknown table")
        else:
            logger.exception("Error execute query: %s", query)
        raise ProgrammingError(str(exc), orig=exc.orig if exc.orig is not None else exc, params=exc.params)
    except Exception as exc:
        logger.exception("Error execute query: %s", query)
        raise Exception(str(exc))
