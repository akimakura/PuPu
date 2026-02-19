"""
Коннектор для ClickHouse
"""

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from starlette_context import context

from src.config import GeneratorConnectionTypeEnum, settings
from src.models.database import ConnetionTypeEnum, Database
from src.utils.cert import get_certs_by_database, get_clickhouse_verify_by_database


async def get_datagate_client(database: Database) -> AsyncClient:
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
    ca_cert, client_cert, client_cert_key, _ = get_certs_by_database(database)
    verify = get_clickhouse_verify_by_database(database)
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


async def get_physical_client(database: Database) -> AsyncClient:
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
    user_key = f"DB_{database.tenant_id}_{database.name}_USER".upper()
    user_pass_key = f"DB_{database.tenant_id}_{database.name}_PASSWORD".upper()
    try:
        username = getattr(settings, user_key)
        password = getattr(settings, user_pass_key)
    except AttributeError:
        raise AttributeError(f"""Configuration key "{user_key}" or "{user_pass_key}" not found in settings.""")
    for connection in database.connections:
        if connection.type != ConnetionTypeEnum.DATAGATE:
            host = connection.host
            port = connection.ports[0].port
            break
    if not host or not port:
        raise ValueError("Clickhouse host or port is not assigned")
    ca_cert, client_cert, client_cert_key, _ = get_certs_by_database(database)
    verify = get_clickhouse_verify_by_database(database)
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


async def get_client(database: Database) -> AsyncClient:
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
    db_type_key = f"DB_{database.tenant_id}_{database.name}_TYPE".upper()
    try:
        db_type = getattr(settings, db_type_key)
    except AttributeError:
        raise AttributeError(f"""Configuration key "{db_type}" not found in settings.""")
    if db_type == GeneratorConnectionTypeEnum.PHYSICAL:
        return await get_physical_client(database)
    if db_type == GeneratorConnectionTypeEnum.DATAGATE:
        return await get_datagate_client(database)
    raise ValueError(f"Unknown database type: {db_type}")
