from ssl import SSLContext, create_default_context
from typing import Optional

from src.config import settings
from src.models.database import Database
from src.utils.validators import validate_path, get_bool_from_str_or_bool
from pydantic import FilePath


def get_clickhouse_verify_by_database(database: Database) -> bool | str:
    """
    Возвращает значение verify (настройка проверки сертификатов) для базы данных clickhouse

    Returns:
        bool | str: Возвращает флаг включение или отключения проверки сертификатов или строку "proxy" в случае proxy.
    """
    db_verify_key = f"DB_{database.tenant_id}_{database.name}_VERIFY".upper()
    db_verify = getattr(settings, db_verify_key, "1")
    if db_verify == "proxy":
        return db_verify
    flag = get_bool_from_str_or_bool(db_verify)
    return flag


def get_certs_by_database(database: Database, is_semantic: bool = False, node_label: str = "") -> tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    """Получить пути до сертификатов по базе данных."""
    node_label = "" if node_label == "0" else node_label
    if not is_semantic:
        ca_cert_var = f"DB{node_label}_{database.tenant_id}_{database.name}_PATH_TO_CA_CERT".upper()
        client_cert_var = f"DB{node_label}_{database.tenant_id}_{database.name}_PATH_TO_CLIENT_CERT".upper()
        client_cert_key_var = f"DB{node_label}_{database.tenant_id}_{database.name}_PATH_TO_CLIENT_CERT_KEY".upper()
        client_cert_password_var = f"DB{node_label}_{database.tenant_id}_{database.name}_CLIENT_CERT_PASSWORD".upper()
    else:
        ca_cert_var = f"DB{node_label}_PATH_TO_CA_CERT"
        client_cert_var = f"DB{node_label}_PATH_TO_CLIENT_CERT".upper()
        client_cert_key_var = f"DB{node_label}_PATH_TO_CLIENT_CERT_KEY".upper()
        client_cert_password_var = f"DB{node_label}_CLIENT_CERT_PASSWORD".upper()
    ca_cert = validate_path(getattr(settings, ca_cert_var, None), ca_cert_var)
    client_cert = validate_path(getattr(settings, client_cert_var, None), client_cert_var)
    client_cert_key = validate_path(getattr(settings, client_cert_key_var, None), client_cert_key_var)
    client_cert_password = getattr(settings, client_cert_password_var, None)
    return ca_cert, client_cert, client_cert_key, client_cert_password


def get_ssl_mode_by_database(database: Database, is_semantic: bool = False, node_label: str = "") -> Optional[str]:
    """Возвращает sslmode для базы данных"""
    if not is_semantic:
        ssl_mode = f"DB_{database.tenant_id}_{database.name}_CONNECTION_{node_label}_SSLMODE".upper()
    else:
        node_label = "" if node_label == "0" else node_label
        ssl_mode = f"DB{node_label}_SSLMODE"
    return getattr(settings, ssl_mode, None)


def get_ssl_context_by_certs(
    ca_cert: Optional[str] = None,
    client_cert: Optional[str] = None,
    client_cert_key: Optional[str] = None,
    cert_password: Optional[str] = None,
) -> Optional[SSLContext]:
    """Преобразовать пути до сертификатов в SSLContext."""
    ssl_context = create_default_context()
    ca_cert = validate_path(ca_cert)
    client_cert = validate_path(client_cert)
    client_cert_key = validate_path(client_cert_key)
    if not client_cert and not ca_cert:
        return None
    if ca_cert:
        ssl_context.load_verify_locations(ca_cert)
    if client_cert:
        ssl_context.load_cert_chain(client_cert, client_cert_key, cert_password)
    return ssl_context


def get_ssl_context_by_database(
    database: Database, is_semantic: bool = False, node_label: str = ""
) -> Optional[SSLContext]:
    """Получить SSLContext по базе данных."""
    ca_cert, client_cert, client_cert_key, client_cert_password = get_certs_by_database(
        database, is_semantic, node_label
    )
    return get_ssl_context_by_certs(ca_cert, client_cert, client_cert_key, client_cert_password)


def get_cert_and_verify_for_httpx(
    ca_cert: Optional[FilePath] = None,
    client_cert: Optional[FilePath] = None,
    client_cert_key: Optional[FilePath] = None,
    cert_password: Optional[str] = None,
) -> tuple[Optional[str | tuple], Optional[str]]:
    """Формирует cert и verify для клиента из httpx"""
    cert: Optional[str | tuple] = None
    verify: Optional[str] = None
    if client_cert is not None and client_cert_key is not None and cert_password is not None:
        cert = (str(client_cert), str(client_cert_key), cert_password)
    elif client_cert is not None and client_cert_key is not None:
        cert = (str(client_cert), str(client_cert_key))
    elif client_cert is not None:
        cert = str(client_cert)
    if ca_cert:
        verify = str(ca_cert)
    return cert, verify
