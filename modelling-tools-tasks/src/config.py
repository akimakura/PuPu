"""
Конфигурации проекта.
"""

import os
import sys
from typing import Any
from urllib.parse import quote_plus

from py_common_lib.consts import ENV_FILE
from pydantic import Field, FilePath, field_validator
from pydantic_core import core_schema
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.integrations.modelling_tools_api.codegen import Configuration as MTApiConfiguration


class Settings(BaseSettings):
    DB_ECHO: bool = False
    BIND_IP: str = "0.0.0.0"
    BIND_PORT: int = 8000
    RESULT_EX_TIME: int = 60 * 60 * 24
    HOSTNAME: str = "default"
    BACKEND_CORS_ORIGINS: str = "*"
    RELOAD: bool = False
    ENABLE_GENERATE_OBJECTS: bool = True
    SYNC_CLICKHOUSE_DROP: bool = True
    DEFAULT_MIGRATE_TENANT_NAME: str = "tenant1"
    # ==== Настройки работы с Кэшем ====
    REDIS_PATH_TO_CA_CERT: FilePath | None = None
    REDIS_PATH_TO_CLIENT_CERT: FilePath | None = None
    REDIS_PATH_TO_CLIENT_CERT_KEY: FilePath | None = None
    REDIS_HOST: str = "127.0.0.1"
    REDIS_PORT: str = "6379"
    REDIS_USER: str = ""
    REDIS_PASSWORD: str = ""
    REDIS_DB: str = ""
    REDIS_URL: str = Field(default="", validate_default=True)
    DATAGATE_USER: str = "jwt"
    DATAGATE_PASSWORD: str = ""
    MT_API_HOST: str = "http://localhost:8001"
    MT_API_TIMEOUT: int = 600
    MT_API_RETRY_COUNT: int = 3
    MT_API_CONFIGURATION: MTApiConfiguration | None = Field(default=None, validate_default=True)
    # ==== Настройки логирования ====
    EPMPY_LOG_LEVEL: str = "DEBUG"
    LOGGING_PATH: str = os.path.join(
        getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(str(__package__)))),
        "conf/logging.conf.yml",
    )
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding="utf-8", extra="allow", case_sensitive=True)

    @field_validator("REDIS_URL")
    @classmethod
    def validate_redis_url(cls, value: str, values: core_schema.ValidationInfo, **kwargs: Any) -> str:
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
        raise ValueError(
            "Redis URL is not valid. Make sure that you have specified the correct host and port. Please check your settings."
        )

    @field_validator("MT_API_CONFIGURATION")
    @classmethod
    def validate_mt_api_config(
        cls, value: str, values: core_schema.ValidationInfo, **kwargs: Any
    ) -> MTApiConfiguration:
        configuration = MTApiConfiguration(
            host=values.data.get("MT_API_HOST"), retries=int(values.data.get("MT_API_RETRY_COUNT", 3))
        )
        configuration.verify_ssl = False
        return configuration


settings = Settings()
