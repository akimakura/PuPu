"""
Коннектор для ClickHouse
"""

from enum import StrEnum
from typing import Any, Optional, Sequence

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import OperationalError
from py_common_lib.logger import EPMPYLogger
from py_common_lib.logger.audit_types import F9
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from starlette_context import context

from src.config import settings
from src.db.utils import get_ip_address_by_dns_name
from src.integrations.modelling_tools_api.codegen.models import Database
from src.models.connection import ConnetionTypeEnum
from src.utils.cert import get_certs_by_database, get_clickhouse_verify_by_database

logger = EPMPYLogger(__name__)


class GeneratorConnectionTypeEnum(StrEnum):
    PHYSICAL = "PHYSICAL"
    DATAGATE = "DATAGATE"


async def get_datagate_client(tenant_id: str, database: Database) -> AsyncClient:
    """
    Получить подключение к clickhouse через datagate.

    Args:
        database (Database): База данных, к которой произвести соединение через datagate.

    Returns:
        AsyncClient: клиент подключения к Clickhouse через datagate.

    Raises:
        ValueError: Если нет хоста и порта базы данных или не передан jwt.
    """
    host = None
    port = None
    if database.connections is None:
        raise ValueError("Connections not found!")
    for connection in database.connections:
        if connection.type == ConnetionTypeEnum.DATAGATE:
            host = connection.host
            port = connection.ports[0].port
            break
    if not host or not port:
        raise ValueError("Clickhouse host or port is not assigned")
    token = context.data.get(AuthorizationPlugin.key)
    if not token:
        raise ValueError("JWT token not found!")
    token = token.replace("Bearer ", "")
    ca_cert, client_cert, client_cert_key, _ = get_certs_by_database(tenant_id, database)
    verify = get_clickhouse_verify_by_database(tenant_id, database)
    return await clickhouse_connect.get_async_client(
        host=host,
        port=port,
        verify=verify,
        client_cert=client_cert,
        client_cert_key=client_cert_key,
        ca_cert=ca_cert,
        username=settings.DATAGATE_USER,
        password=settings.DATAGATE_PASSWORD,
        generic_args={"datagate_mode": "default", "datagate_jwt": token},
    )


async def get_physical_client(tenant_id: str, database: Database) -> AsyncClient:
    """
    Получить подключение к clickhouse напрямую.

    Args:
        database (Database): База данных, к которой произвести соединение напрямую.
        tenant: (Optional[str]): id тенанта, в котором находится модель базы данных.

    Returns:
        AsyncClient: клиент подключения к Clickhouse напрямую.

    Raises:
        ValueError: Если нет хоста и порта базы данных.
        AttributeError: Если в .env нет DB_{tenant}_{database.name}_USER или DB_{tenant}_{database.name}_PASSWORD
    """
    host = None
    port = None
    user_key = f"DB_{tenant_id}_{database.name}_USER".upper()
    user_pass_key = f"DB_{tenant_id}_{database.name}_PASSWORD".upper()
    try:
        username = getattr(settings, user_key)
        password = getattr(settings, user_pass_key)
    except AttributeError:
        raise AttributeError(f"""Configuration key "{user_key}" or "{user_pass_key}" not found in settings.""")
    if database.connections is None:
        raise ValueError("Connections not found!")
    for connection in database.connections:
        if connection.type != ConnetionTypeEnum.DATAGATE:
            host = connection.host
            port = connection.ports[0].port
            break
    if not host or not port:
        raise ValueError("Clickhouse host or port is not assigned")
    ca_cert, client_cert, client_cert_key, _ = get_certs_by_database(tenant_id, database)
    verify = get_clickhouse_verify_by_database(tenant_id, database)
    return await clickhouse_connect.get_async_client(
        host=host,
        port=port,
        verify=verify,
        client_cert=client_cert,
        client_cert_key=client_cert_key,
        ca_cert=ca_cert,
        username=username,
        password=password,
    )


async def get_client(tenant_id: str, database: Database) -> AsyncClient:
    """
    Получить подключение к clickhouse.

    Args:
        database (Database): База данных, к которой произвести соединение напрямую.
    Returns:
        AsyncClient: клиент подключения к Clickhouse.

    Raises:
        ValueError: Если передан неизвестный тип базы данных.
        AttributeError: Если в .env нет DB_{tenant}_{database.name}_TYPE
    """
    db_type_key = f"DB_{tenant_id}_{database.name}_TYPE".upper()
    db_type = "unknown"
    try:
        db_type = getattr(settings, db_type_key)
    except AttributeError:
        raise AttributeError(f"""Configuration key "{db_type}" not found in settings.""")
    if db_type == GeneratorConnectionTypeEnum.PHYSICAL:
        return await get_physical_client(tenant_id, database)
    if db_type == GeneratorConnectionTypeEnum.DATAGATE:
        return await get_datagate_client(tenant_id, database)
    raise ValueError(f"Unknown database type: {db_type}")


async def execute_raw_DDL(tenant_id: str, database: Database, queries: str | list[str]) -> None:
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
    query = ""
    try:
        client = await get_client(tenant_id, database)
        if isinstance(queries, list):
            for query in queries:
                logger.debug("""EXECUTE QUERY: "%s";""", query)
                await client.query(query)
        else:
            logger.debug("""EXECUTE QUERY: "%s";""", queries)
            await client.query(queries)
        return None
    except OperationalError as ext:
        ip_address = get_ip_address_by_dns_name(database.connections[0].host)
        logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
        raise OperationalError(str(ext))
    except ConnectionRefusedError as ext:
        ip_address = get_ip_address_by_dns_name(database.connections[0].host)
        logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
        raise ConnectionRefusedError(str(ext))
    except Exception as ext:
        logger.error("Error executing DDL: %s", query)
        raise Exception(str(ext))


async def execute_raw_DQL_or_DML(
    tenant_id: str, database: Database, query: str, params: Optional[dict[Any, Any]] = None
) -> list[Sequence[Any]]:
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
        client = await get_client(tenant_id, database)
        logger.debug("""EXECUTE QUERY: "%s";""", query)
        result_ch = await client.query(query)
        return list(result_ch.result_rows)
    except OperationalError as ext:
        ip_address = get_ip_address_by_dns_name(database.connections[0].host)
        logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
        raise OperationalError(str(ext))
    except ConnectionRefusedError as ext:
        ip_address = get_ip_address_by_dns_name(database.connections[0].host)
        logger.audit(F9, host=ip_address, dns_name=database.connections[0].host)
        raise ConnectionRefusedError(str(ext))
    except Exception as exc:
        if "(UNKNOWN_TABLE)" in str(exc):
            logger.warning("Unknown table")
        else:
            logger.exception("Error execute query: %s", query)
        raise Exception(exc)
