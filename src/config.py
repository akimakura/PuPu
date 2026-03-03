"""
Конфигурации проекта.
"""

import json
import os
import sys
from enum import StrEnum
from typing import Optional
from urllib.parse import quote_plus
from uuid import UUID

from py_common_lib.consts import ENV_FILE
from py_common_lib.logger import EPMPYLogger
from pydantic import Field, FilePath, SecretStr, field_validator
from pydantic_core import core_schema
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = EPMPYLogger(__name__)


class GeneratorConnectionTypeEnum(StrEnum):
    PHYSICAL = "PHYSICAL"
    DATAGATE = "DATAGATE"


def read_static_file_as_dict(path_to_file: str) -> dict:
    with open(path_to_file, "r", encoding="utf-8") as file:
        static_dict: dict = json.load(file)
    return static_dict


def try_hide_write_api(settings: "Settings") -> None:
    """Попробовать скрыть все эндпоинты, которые изменяют данные."""
    if settings.HIDE_HIERARCHY_WRITE_API:
        settings.HIDE_UPDATE_HIERARCHY = True
        settings.HIDE_DELETE_HIERARCHY = True
        settings.HIDE_CREATE_HIERARCHY = True
        settings.HIDE_COPY_HIERARCHIES_TO_ANOTHER_MODEL = True
        settings.HIDE_UPDATE_HIERARCHY_BY_NAME = True
        settings.HIDE_DELETE_HIERARCHY_BY_NAME = True
        settings.HIDE_CREATE_HIERARCHY_IN_PVD = True
        settings.HIDE_UPDATE_HIERARCHY_IN_PVD = True
        settings.HIDE_DELETE_HIERARCHY_IN_PVD = True
    if not settings.HIDE_WRITE_API:
        return None
    settings.HIDE_CREATE_IN_AOR = True
    settings.HIDE_CREATE_MODEL_IN_AOR = True
    settings.HIDE_CHANGE_OBJECT_STATUS = True
    settings.HIDE_CREATE_COMPOSITE = True
    settings.HIDE_DELETE_COMPOSITE_BY_NAME = True
    settings.HIDE_UPDATE_COMPOSITE_BY_NAME = True
    settings.HIDE_COPY_MODEL_COMPOSITE = True
    settings.HIDE_CREATE_DATA_STORAGE = True
    settings.HIDE_DELETE_DATA_STORAGE_BY_NAME = True
    settings.HIDE_UPDATE_DATA_STORAGE_BY_NAME = True
    settings.HIDE_CREATE_MODEL_DATASTORAGE = True
    settings.HIDE_DELETE_DATABASE_BY_NAME = True
    settings.HIDE_CREATE_DATABASE = True
    settings.HIDE_UPDATE_DATABASE_BY_NAME = True
    settings.HIDE_CREATE_DIMENSION = True
    settings.HIDE_UPDATE_DIMENSION_BY_NAME = True
    settings.HIDE_DELETE_DIMENSION_BY_NAME = True
    settings.HIDE_CREATE_DIMENSION_IN_PVD = True
    settings.HIDE_UPDATE_DIMENSION_IN_PVD = True
    settings.HIDE_CREATE_DIMENSIONS_IN_PVD = True
    settings.HIDE_DELETE_DIMENSION_IN_PVD = True
    settings.HIDE_CREATE_MODEL_DIMENSION = True
    settings.HIDE_COPY_MODEL_DIMENSION = True
    settings.HIDE_CREATE_HIERARCHY = True
    settings.HIDE_DELETE_MEASURE_BY_NAME = True
    settings.HIDE_UPDATE_MEASURE_BY_NAME = True
    settings.HIDE_CREATE_MEASURE = True
    settings.HIDE_COPY_MODEL_MEASURE = True
    settings.HIDE_CREATE_DATA_STORAGE_IN_DATABASE_FROM_META = True
    settings.HIDE_CREATE_DATA_STORAGES_IN_DATABASE_FROM_META = True
    settings.HIDE_CREATE_COMPOSITES_IN_DATABASE_FROM_META = True
    settings.HIDE_CREATE_COMPOSITE_IN_DATABASE_FROM_META = True
    settings.HIDE_COPY_MODEL_DATA_STORAGE = True
    settings.HIDE_UPDATE_MODEL_BY_NAME = True
    settings.HIDE_CREATE_MODEL = True
    settings.HIDE_DELETE_MODEL_BY_NAME = True
    settings.HIDE_UPDATE_TENANT_BY_NAME = True
    settings.HIDE_CREATE_TENANT = True
    settings.HIDE_DELETE_TENANT_BY_NAME = True
    settings.HIDE_INVALIDATE_CACHES = True


class Settings(BaseSettings):
    APP_SECRET_KEY: Optional[SecretStr] = None
    APP_SECRET_HEADER: str = "X-ACCESS-TOKEN"
    BIND_IP: str = "0.0.0.0"
    BIND_PORT: int = 8000
    HOSTNAME: str = "default"
    BACKEND_CORS_ORIGINS: str = "*"
    HIDE_WRITE_API: bool = False
    HIDE_HIERARCHY_WRITE_API: bool = False
    UVICORN_PATH_TO_CA_CERT: Optional[FilePath] = None
    UVICORN_WORKERS_COUNT: int = 1
    UVICORN_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    UVICORN_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    UVICORN_CERT_PASSWORD: Optional[str] = None
    UVICORN_TIMEOUT_GRACEFULL_SHUTDOWN: int = 30
    SQLALCHEMY_POOL_PRE_PING: bool = True
    SQLALCHEMY_POOL_RECYCLE: int = 3600
    SQLALCHEMY_POOL_SIZE: int = 10
    SQLALCHEMY_MAX_OVERFLOW: int = 20
    SQLAlCHEMY_POOL_TIMEOUT: int = 30
    # ==== Настройки логирования ====
    LOGGING_LEVEL: str = "DEBUG"
    LOGGING_PATH: str = os.path.join(
        getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__package__))),
        "conf/logging.conf.yml",
    )
    ENABLE_IGNORING_DIMENSIONS: bool = True
    MODELS_BLACKLIST: set[str] = {"nsi_prestage"}
    DIMENSIONS_WHITELIST: set[str] = {"operation"}

    # ==== Настройки работы с БД ====
    TEST_TOKEN_KEY: str = "test"
    ENABLE_MASTER_SELECTION: bool = True
    COUNT_DATABASE_NODES: int = 101
    DB_USER: str = "user"
    DB_USER_WORK: str = "semantic_layer_work"
    DB_PASS: str = "pass"
    DB_HOST: str = "host"
    DB_PORT: str = "5432"
    DB_NAME: str = "semantic_layer"
    DB_SCHEMA: str = "semantic_layer"
    DB_SSLMODE: str = "disable"
    DB_URL: str = Field(default="", validate_default=True)
    DB_PATH_TO_CA_CERT: Optional[FilePath] = None
    DB_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    DB_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    DB_CLIENT_CERT_PASSWORD: Optional[str] = None
    DB_ECHO: bool = True
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="allow", case_sensitive=True)
    CACHE_PREFIX: str = "semantic_layer_api"
    ENABLE_GENERATE_OBJECTS: bool = False
    ENABLE_COLLECT_VIEW_FOR_DS: bool = True
    ENABLE_SWITCH_MODEL_SCHEMA: bool = True
    ENABLE_LEGACY_MODEL_SCHEMA_OVERRIDE: bool = True
    DATAGATE_USER: str = "jwt"
    DATAGATE_PASSWORD: str = ""
    # Включить помену хостов на хосты из .env
    ENABLE_SWITCH_HOST: bool = True
    # ==== Настройки работы с Кэшем ====
    REDIS_PATH_TO_CA_CERT: Optional[FilePath] = None
    REDIS_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    REDIS_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    REDIS_HOST: str = ""
    REDIS_PORT: str = ""
    REDIS_USER: str = ""
    REDIS_PASSWORD: str = ""
    REDIS_DB: str = ""
    REDIS_URL: str = Field(default="", validate_default=True)
    # ==== Настройки Singleflight (защита от thundering herd) ====
    ENABLE_SINGLEFLIGHT: bool = True
    SINGLEFLIGHT_LOCK_TTL: int = 30
    SINGLEFLIGHT_WAIT_TIMEOUT: float = 10.0
    SINGLEFLIGHT_POLL_INTERVAL: float = 0.1
    # ==== паттерны создания имен DataStorage ====
    VALUES_DATASTORAGE_PATTERN: str = "%s_values"
    TEXT_DATASTORAGE_PATTERN: str = "%s_texts"
    ATTRIBUTE_DATASTORAGE_PATTERN: str = "%s_attributes"
    DISTRIBUTED_TABLE_PATTERN: str = "%s_distr"
    DICTIONARY_TABLE_PATTERN: str = "%s_d"
    HIERARCHY_TABLE_PATTERN: str = "%s_hier"
    GENERATE_SCHEMA_NAME: str = "MD"
    LOGS_TABLE_PATTERN: str = "%s_logs"

    # ==== Путь до статики ====
    PATH_TO_STATIC: str = os.path.join(
        getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__package__))),
        "src/static",
    )

    # ==== Миграции ===========
    ENABLE_SCHEMA_MIGRATIONS: bool = False
    PATH_TO_SCHEMA_MIGRATIONS: str = os.path.join(
        getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__package__))),
        "migrations",
    )
    PATH_TO_ALEMBIC_INI_SCHEMA: str = os.path.join(
        getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__package__))),
        "conf/alembic_migrations.ini",
    )

    ENABLE_ROR_DEV_LT_MIGRATIONS: bool = False
    ROR_DEV_LT_MIGRATIONS_MODEL_NAME: str = "ror_dev_lt"
    ROR_DEV_LT_MIGRATIONS_TENANT_NAME: str = "tenant1"
    ENABLE_NSI_STAGE_MIGRATIONS: bool = False
    NSI_STAGE_MIGRATIONS_MODEL_NAME: str = "nsi_stage"
    NSI_STAGE_MIGRATIONS_TENANT_NAME: str = "tenant1"
    ENABLE_NSI_MART_MIGRATIONS: bool = False
    NSI_MART_MIGRATIONS_MODEL_NAME: str = "nsi_mart"
    NSI_MART_MIGRATIONS_TENANT_NAME: str = "tenant1"

    ALEMBIC_TARGET_REVISION: str = "head"
    ALEMBIC_ACTION: str = "upgrade"

    # === Настройки кликхауса ==
    DEFAULT_DICTIONARY_PORT: int = 9000
    DEFAULT_DICTIONARY_HOST: str = "localhost"
    SYNC_CLICKHOUSE_DROP: bool = True

    # === Скрытие эндпоинтов ===
    HIDE_CHANGE_OBJECT_STATUS: bool = False
    HIDE_GET_COMPOSITE_LIST_BY_MODEL_NAME: bool = False
    HIDE_GET_COMPOSITE_BY_COMPOSITE_NAME: bool = False
    HIDE_CREATE_COMPOSITE: bool = False
    HIDE_DELETE_COMPOSITE_BY_NAME: bool = False
    HIDE_UPDATE_COMPOSITE_BY_NAME: bool = False
    HIDE_COPY_MODEL_COMPOSITE: bool = False
    HIDE_GET_DATA_STORAGE_LIST_BY_MODEL_NAME: bool = False
    HIDE_GET_DATA_STORAGE_BY_DS_NAME: bool = False
    HIDE_GET_DATA_STORAGE_BY_DB_OBJECT: bool = False
    HIDE_CREATE_DATA_STORAGE: bool = False
    HIDE_DELETE_DATA_STORAGE_BY_NAME: bool = False
    HIDE_UPDATE_DATA_STORAGE_BY_NAME: bool = False
    HIDE_COPY_MODEL_DATA_STORAGE: bool = False
    HIDE_CREATE_MODEL_DATASTORAGE: bool = False
    HIDE_GET_DATABASE_LIST: bool = False
    HIDE_GET_DATABASE_BY_NAME: bool = False
    HIDE_DELETE_DATABASE_BY_NAME: bool = False
    HIDE_CREATE_DATABASE: bool = False
    HIDE_UPDATE_DATABASE_BY_NAME: bool = False
    HIDE_GET_DIMENSION_LIST_BY_MODEL_NAME: bool = False
    HIDE_GET_DIMENSION_BY_DIMENSION_NAME: bool = False
    HIDE_GET_DIMENSION_BY_NAMES: bool = False
    HIDE_DELETE_DIMENSION_BY_NAME: bool = False
    HIDE_CREATE_DIMENSION: bool = False
    HIDE_UPDATE_DIMENSION_BY_NAME: bool = False
    HIDE_CREATE_DIMENSION_IN_PVD: bool = False
    HIDE_UPDATE_DIMENSION_IN_PVD: bool = False
    HIDE_CREATE_DIMENSIONS_IN_PVD: bool = False
    HIDE_DELETE_DIMENSION_IN_PVD: bool = False
    HIDE_CREATE_MODEL_DIMENSION: bool = False
    HIDE_COPY_MODEL_DIMENSION: bool = False
    HIDE_GET_HIERARCHY_BY_HIERARCHY_NAME: bool = False
    HIDE_GET_HIERARCHY_BY_HIERARCHY_NAME_WITHOUT_DIMENSIONS: bool = False
    HIDE_GET_HIERARCHY_BY_HIERARCHY_NAMES: bool = False
    HIDE_GET_HIERARCHY_BY_HIERARCHIES: bool = False
    HIDE_GET_HIERARCHY_BY_HIERARCHIES_AND_DIMENSION: bool = False
    HIDE_GET_HIERARCHY_DIMENSION_AND_HIERARCHY_NAME: bool = False
    HIDE_UPDATE_HIERARCHY: bool = False
    HIDE_DELETE_HIERARCHY: bool = False
    HIDE_CREATE_HIERARCHY: bool = False
    HIDE_DELETE_HIERARCHY_BY_NAME: bool = False
    HIDE_UPDATE_HIERARCHY_BY_NAME: bool = False
    HIDE_CREATE_HIERARCHY_IN_PVD: bool = False
    HIDE_UPDATE_HIERARCHY_IN_PVD: bool = False
    HIDE_DELETE_HIERARCHY_IN_PVD: bool = False
    HIDE_COPY_HIERARCHIES_TO_ANOTHER_MODEL: bool = False
    HIDE_GET_MEASURE_LIST_BY_MODEL_NAME: bool = False
    HIDE_GET_MEASURE_LIST_BY_NAMES: bool = False
    HIDE_GET_MEASURE_BY_MEASURE_NAME: bool = False
    HIDE_COPY_MODEL_MEASURE: bool = False
    HIDE_CREATE_MEASURE: bool = False
    HIDE_DELETE_MEASURE_BY_NAME: bool = False
    HIDE_UPDATE_MEASURE_BY_NAME: bool = False
    HIDE_GET_MODEL_LIST: bool = False
    HIDE_GET_MODEL_BY_NAME: bool = False
    HIDE_DELETE_MODEL_BY_NAME: bool = False
    HIDE_CREATE_MODEL: bool = False
    HIDE_UPDATE_MODEL_BY_NAME: bool = False
    HIDE_CREATE_DATA_STORAGES_IN_DATABASE_FROM_META: bool = False
    HIDE_CREATE_DATA_STORAGE_IN_DATABASE_FROM_META: bool = False
    HIDE_CREATE_COMPOSITES_IN_DATABASE_FROM_META: bool = False
    HIDE_CREATE_COMPOSITE_IN_DATABASE_FROM_META: bool = False
    HIDE_GET_TENANT_LIST: bool = False
    HIDE_GET_TENANT_BY_NAME: bool = False
    HIDE_DELETE_TENANT_BY_NAME: bool = False
    HIDE_CREATE_TENANT: bool = False
    HIDE_UPDATE_TENANT_BY_NAME: bool = False
    HIDE_INVALIDATE_CACHES: bool = False
    HIDE_GET_SEMANTIC_PERMISSIONS: bool = False
    HIDE_CREATE_IN_AOR: bool = False
    HIDE_DEPLOY_BY_AOR: bool = False
    HIDE_CREATE_MODEL_IN_AOR: bool = False
    # = настройки PVDictionary ==
    ENABLE_PV_DICTIONARIES: bool = True
    PV_DICTIONARIES_URL: str = ""
    PV_DICTIONARIES_FRONT_URL: str = ""
    PV_DICTIONARIES_FRONT_GET_DICTIONARY: str = "/mdc/#/{}/dicts/"
    PV_DICTIONARIES_GET_DICTIONARY: str = "/mdc-api/rn/%s/dictionary"
    PV_DICTIONARIES_DELETE_DICTIONARY: str = "/mdc-api/v2/rn/%s/dictionary/byName/"
    PV_DICTIONARIES_CREATE_DICTIONARY: str = "/mdc-api/rn/%s/dictionary/model"
    PV_DICTIONARIES_CREATE_VERSION: str = "/mdc-api/v1/rn/%s/dictionary/%s/versions"
    PV_DICTIONARIES_ACTIVATE_VERSION: str = "/mdc-api/v1/rn/%s/dictionary/%s/versions/%s/state"
    PV_DICTIONARIES_MODEL_VERSION: str = "1.1"
    PV_DICTIONARIES_DICTIONARY_CREATE_VERSION: str = "1.0"
    PV_DICTIONARIES_MODEL_XMLNS: str = "http://sberbank.ru/schema/mvnsi/mdmp/model"
    PV_DICTIONARIES_VERSION_CODE: str = "1.1"
    PV_DICTIONARIES_PATH_TO_CA_CERT: Optional[FilePath] = None
    PV_DICTIONARIES_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    PV_DICTIONARIES_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    PV_DICTIONARIES_CLIENT_CERT_PASSWORD: Optional[str] = None
    PV_DICTIONARIES_AUTH_HEADER: str = "Authorization"
    PV_DICTIONARIES_DEFAULT_DOMAIN_NAME: str = "test_semantica_create"
    PV_DICTIONARIES_DEFAULT_NAME_PATTERN: str = "df_%s"
    PV_DICTIONARIES_DEFAULT_NAME_SUFFIX: str = ""
    PV_DICTIONARIES_DEFAULT_DOMAIN_LABEL: str = "Тестирование Сем. слоя"
    PV_DICTIONARIES_TENANT_NAME: str = "EPM"
    PV_DICTIONARIES_VERSIONED_DICTIONARY: bool = False
    PV_DICTIONARIES_VERSION_DESCRIPTION_PATTERN: str = "Изменение справочника от %s"
    # = настройки PVD Hierarchies ==
    ENABLE_PV_HIERARCHIES_META_SYNC: bool = True
    PV_HIERARCHIES_CREATE_URL: str = "/hierarchy-rest/api/v1/hierarchies/create"
    PV_HIERARCHIES_UPDATE_URL: str = "/hierarchy-rest/api/v1/hierarchies/%s"
    PV_HIERARCHIES_DELETE_URL: str = "/hierarchy-rest/api/v1/hierarchies/%s"
    # = настройки Kafka =======
    ENABLE_KAFKA: bool = False
    KAFKA_SERVERS: list[str] = []
    KAFKA_UPDATE_ENTITIES_TOPIC: str = "sl.updating_entities"
    KAFKA_CLIENT_ID: str = "semantic-layer"
    KAFKA_PATH_TO_CA_CERT: Optional[FilePath] = None
    KAFKA_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    KAFKA_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    KAFKA_CLIENT_CERT_PASSWORD: Optional[str] = None

    # Настройки worker manager
    WORKER_MANAGER_URL: str = "http://localhost:8000"
    WORKER_DATASTORAGE_URL: str = "api/v0/tenants/{tenantName}/datastorages/"
    WORKER_DIMENSION_URL: str = "api/v0/tenants/{tenantName}/dimensions/"
    WORKER_COMPOSITE_URL: str = "api/v0/tenants/{tenantName}/composites/"
    WORKER_PATH_TO_CA_CERT: Optional[FilePath] = None
    WORKER_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    WORKER_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    WORKER_CLIENT_CERT_PASSWORD: Optional[str] = None
    WORKER_AUTH_HEADER: str = "Authorization"

    # Настройки Aor client
    ENABLE_AOR: bool = False
    AOR_URL: str = "http://localhost:8002"
    AOR_HTTP_TIMEOUT: int = 60
    AOR_PUSH_URL: str = "/api/v1/aor/push_object"
    AOR_SECRET_KEY: SecretStr = SecretStr("")
    AOR_PATH_TO_CA_CERT: Optional[FilePath] = None
    AOR_PATH_TO_CLIENT_CERT: Optional[FilePath] = None
    AOR_PATH_TO_CLIENT_CERT_KEY: Optional[FilePath] = None
    AOR_CLIENT_CERT_PASSWORD: Optional[str] = None
    AOR_AUTH_HEADER: str = "Authorization"
    AOR_ACCESS_HEADER: str = "X-ACCESS-TOKEN"
    AOR_SERVICE_UUID: Optional[UUID] = None
    AOR_SEMANTIAC_SPACE_UUID: Optional[UUID] = None
    DIMENSION_OWNER_PRIORITY: list[str] = ["nsi_mart", "nsi_stage", "nsi_prestage", "nsi_snap"]

    @field_validator("REDIS_URL")
    @classmethod
    def validate_redis_url(cls, value: str, values: core_schema.ValidationInfo, **kwargs: dict) -> str:
        if value:
            return value
        ca_cert = values.data.get("REDIS_PATH_TO_CA_CERT", "")
        client_cert = values.data.get("REDIS_PATH_TO_CLIENT_CERT", "")
        client_cert_key = values.data.get("REDIS_PATH_TO_CLIENT_CERT_KEY", "")
        redis_schema = "rediss" if ca_cert or client_cert else "redis"
        ssl_string = None
        ssl_keyfile = f"ssl_keyfile={client_cert_key}" if client_cert_key else None
        ssl_certfile = f"ssl_certfile={client_cert}" if client_cert else None
        ssl_ca_certs = f"ssl_ca_certs={ca_cert}" if ca_cert else None
        for cert in [ssl_ca_certs, ssl_certfile, ssl_keyfile]:
            if not ssl_string and cert:
                ssl_string = cert
            elif ssl_string and cert:
                ssl_string += f"&{cert}"
        redis_db = values.data.get("REDIS_DB", "")
        redis_username = values.data.get("REDIS_USER", "")
        redis_host = values.data.get("REDIS_HOST", "")
        redis_port = values.data.get("REDIS_PORT", "")
        redis_password = values.data.get("REDIS_PASSWORD", "").replace("@", quote_plus("@"))
        if redis_host and redis_port and not redis_password and not redis_username:
            redis_url = f"{redis_schema}://{redis_host}:{redis_port}/{redis_db}"
            redis_url = redis_url + f"?{ssl_string}" if ssl_string else redis_url
            return redis_url
        elif redis_host and redis_port and redis_password and not redis_username:
            redis_url = f"{redis_schema}://{redis_host}:{redis_port}/{redis_db}?password={redis_password}"
            redis_url = redis_url + f"&{ssl_string}" if ssl_string else redis_url
            return redis_url
        elif redis_host and redis_port and not redis_password and redis_username:
            redis_url = f"{redis_schema}://{redis_username}@{redis_host}:{redis_port}/{redis_db}"
            redis_url = redis_url + f"?{ssl_string}" if ssl_string else redis_url
            return redis_url
        elif redis_host and redis_port and redis_password and redis_username:
            redis_url = f"{redis_schema}://{redis_username}:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
            redis_url = redis_url + f"?{ssl_string}" if ssl_string else redis_url
            return redis_url
        return f"{redis_schema}://"


settings = Settings()

try_hide_write_api(settings)

models_limitations = read_static_file_as_dict(os.path.join(settings.PATH_TO_STATIC, "models_limitations.json"))

