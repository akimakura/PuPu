"""Схемы Pydantic для описания Database."""

from enum import StrEnum
from typing import Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator
from pydantic_core import core_schema

from src.config import models_limitations, settings
from src.integration.aor.model import AorType
from src.models.label import Label
from src.models.version import Versioned
from src.utils.validators import get_bool_from_str_or_bool


class DatabaseTypeEnum(StrEnum):
    CLICKHOUSE = "CLICKHOUSE"
    GREENPLUM = "GREENPLUM"
    POSTGRESQL = "POSTGRESQL"


class ProtocolTypeEnum(StrEnum):
    CLICKHOUSE_HTTP = "CLICKHOUSE_HTTP"
    CLICKHOUSE_NATIVE = "CLICKHOUSE_NATIVE"
    POSTGRESQL_V3 = "POSTGRESQL_V3"


class ConnetionTypeEnum(StrEnum):
    LOAD_BALANCER = "LOAD_BALANCER"
    DATAGATE = "DATAGATE"
    NODE = "NODE"


DATABASE_TYPE_TO_PROTOCOL = {
    DatabaseTypeEnum.CLICKHOUSE: ProtocolTypeEnum.CLICKHOUSE_HTTP,
    DatabaseTypeEnum.POSTGRESQL: ProtocolTypeEnum.POSTGRESQL_V3,
    DatabaseTypeEnum.GREENPLUM: ProtocolTypeEnum.POSTGRESQL_V3,
}


class Port(BaseModel):
    """Порт подключения."""

    port: int = Field(
        description=models_limitations["port"]["port"]["description"],
        le=models_limitations["port"]["port"]["le"],
        ge=models_limitations["port"]["port"]["ge"],
    )
    protocol: ProtocolTypeEnum = Field(
        description=models_limitations["port"]["protocol"]["description"],
        max_length=models_limitations["port"]["protocol"]["max_length"],
    )
    sql_dialect: DatabaseTypeEnum = Field(
        description=models_limitations["port"]["sql_dialect"]["description"],
        serialization_alias=models_limitations["port"]["sql_dialect"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["port"]["sql_dialect"]["validation_alias"][0],
            models_limitations["port"]["sql_dialect"]["validation_alias"][1],
        ),
        max_length=models_limitations["port"]["sql_dialect"]["max_length"],
    )
    secured: bool = Field(description=models_limitations["port"]["secured"]["description"], default=False)
    model_config = ConfigDict(from_attributes=True)


class Connection(BaseModel):
    """Подключение к базе данных."""

    host: str = Field(
        description=models_limitations["connection"]["host"]["description"],
        max_length=models_limitations["connection"]["host"]["max_length"],
        pattern=models_limitations["connection"]["host"]["pattern"],
    )
    ports: list[Port] = Field(
        description=models_limitations["connection"]["ports"]["description"],
        max_length=models_limitations["connection"]["ports"]["max_length"],
    )
    type: ConnetionTypeEnum = Field(description=models_limitations["connection"]["type"]["description"])
    model_config = ConfigDict(from_attributes=True)


class Database(Versioned, BaseModel):
    """Схема Базы данных для чтения из кэша и бд."""

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    tenant_id: str = Field(exclude=True, default="")
    type: DatabaseTypeEnum = Field(
        description=models_limitations["database"]["type"]["description"],
        max_length=models_limitations["database"]["type"]["max_length"],
    )
    aor_type: AorType = Field(
        default=AorType.DATABASE,
        serialization_alias=models_limitations["aor_type"]["serialization_alias"],
        validation_alias=AliasChoices(*models_limitations["aor_type"]["validation_alias"]),
    )
    connections: list[Connection] = Field(
        description=models_limitations["database"]["connections"]["description"],
        default=[],
        validate_default=True,
        max_length=models_limitations["database"]["connections"]["max_length"],
    )
    default_cluster_name: Optional[str] = Field(
        default=None,
        description=models_limitations["database"]["default_cluster_name"]["description"],
        min_length=models_limitations["database"]["default_cluster_name"]["min_length"],
        max_length=models_limitations["database"]["default_cluster_name"]["max_length"],
        pattern=models_limitations["database"]["default_cluster_name"]["pattern"],
        serialization_alias=models_limitations["database"]["default_cluster_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["database"]["default_cluster_name"]["validation_alias"][0],
            models_limitations["database"]["default_cluster_name"]["validation_alias"][1],
        ),
        validate_default=True,
    )
    db_name: Optional[str] = Field(
        default=None,
        description=models_limitations["database"]["db_name"]["description"],
        min_length=models_limitations["database"]["db_name"]["min_length"],
        max_length=models_limitations["database"]["db_name"]["max_length"],
        pattern=models_limitations["database"]["db_name"]["pattern"],
        serialization_alias=models_limitations["database"]["db_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["database"]["db_name"]["validation_alias"][0],
            models_limitations["database"]["db_name"]["validation_alias"][1],
        ),
        validate_default=True,
    )
    labels: list[Label] = Field(
        description=models_limitations["database"]["labels"]["description"],
        default=[],
        max_length=models_limitations["database"]["labels"]["max_length"],
    )
    model_config = ConfigDict(from_attributes=True)

    @field_validator("connections")
    @classmethod
    def switch_hosts(
        cls, connections: list[Connection], all_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> list[Connection]:
        """
        Подменяет хосты и порты значениями из .env файла, если включена опция ENABLE_SWITCH_HOST.
        Ищет в .env файле переменные "DB_{tenant}_{name}_CONNECTION_{N}_HOST" и
        DB_{tenant}_{name}_CONNECTION_{N}_PORT_{M}" и заменяет соответствующие значения модели connection.
        """
        if not settings.ENABLE_SWITCH_HOST or not all_fields.data.get("tenant_id"):
            return connections
        tenant = all_fields.data.get("tenant_id")
        name = all_fields.data.get("name")
        database_type = all_fields.data.get("type")
        if database_type is None:
            raise ValueError("Database type could not be None.")
        connections_num_key = f"DB_{tenant}_{name}_CONNECTION_COUNT".upper()
        connection_count = int(getattr(settings, connections_num_key, len(connections)))
        connection_env = "DB_{}_{}_CONNECTION_{}_HOST"
        connection_type_env = "DB_{}_{}_CONNECTION_{}_TYPE"
        port_env = "DB_{}_{}_CONNECTION_{}_PORT_{}"
        protocol_env = "DB_{}_{}_CONNECTION_{}_PORT_{}_PROTOCOL"
        dialect_env = "DB_{}_{}_CONNECTION_{}_PORT_{}_DIALECT"
        secured_env = "DB_{}_{}_CONNECTION_{}_PORT_{}_SECURED"
        if connection_count < len(connections):
            connections = connections[:connection_count]
        for connection_num, connection in enumerate(connections):
            connection.host = getattr(
                settings, (connection_env.format(tenant, name, connection_num)).upper(), connection.host
            )
            connection.type = ConnetionTypeEnum(
                getattr(settings, (connection_type_env.format(tenant, name, connection_num)), connection.type)
            )
            for port_num, port in enumerate(connection.ports):
                port.port = int(
                    getattr(settings, (port_env.format(tenant, name, connection_num, port_num)).upper(), port.port)
                )
                port.protocol = ProtocolTypeEnum(
                    getattr(
                        settings, (protocol_env.format(tenant, name, connection_num, port_num)).upper(), port.protocol
                    )
                )
                port.sql_dialect = DatabaseTypeEnum(
                    getattr(
                        settings, (dialect_env.format(tenant, name, connection_num, port_num)).upper(), port.sql_dialect
                    )
                )
                port.secured = get_bool_from_str_or_bool(
                    getattr(
                        settings, (secured_env.format(tenant, name, connection_num, port_num)).upper(), port.secured
                    )
                )
        if connection_count > len(connections):
            new_connections = []
            for connection_num in range(len(connections), connection_count):
                host = getattr(settings, (connection_env.format(tenant, name, connection_num)).upper())
                type = ConnetionTypeEnum(
                    getattr(settings, (connection_type_env.format(tenant, name, connection_num)).upper())
                )
                port_port = int(getattr(settings, (port_env.format(tenant, name, connection_num, 0)).upper()))
                protocol = ProtocolTypeEnum(
                    getattr(
                        settings,
                        (protocol_env.format(tenant, name, connection_num, 0)).upper(),
                        DATABASE_TYPE_TO_PROTOCOL[database_type],
                    )
                )
                sql_dialect = DatabaseTypeEnum(
                    getattr(settings, (dialect_env.format(tenant, name, connection_num, 0)).upper(), database_type)
                )
                secured = get_bool_from_str_or_bool(
                    getattr(settings, (secured_env.format(tenant, name, connection_num, port_num)).upper(), False)
                )
                new_connections.append(
                    Connection(
                        host=host,
                        type=type,
                        ports=[
                            Port(
                                port=port_port,
                                protocol=protocol,
                                sql_dialect=sql_dialect,
                                secured=secured,
                            )
                        ],
                    )
                )
            connections.extend(new_connections)
        return connections

    @field_validator("default_cluster_name")
    @classmethod
    def switch_cluster(
        cls, default_cluster_name: Optional[str], all_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[str]:
        """
        Подменяет имя кластера из .env файла, если включена опция ENABLE_SWITCH_HOST.
        Ищет в .env файле переменные "DB_{tenant}_{name}_CLUSTER" и заменяет соответствующие значения модели Database.
        """
        if not settings.ENABLE_SWITCH_HOST or not all_fields.data.get("tenant_id"):
            return default_cluster_name
        tenant = all_fields.data.get("tenant_id")
        name = all_fields.data.get("name")
        cluster_name_env = f"DB_{tenant}_{name}_CLUSTER".upper()
        new_cluster_name = getattr(settings, cluster_name_env, None)
        if new_cluster_name == " ":
            return None
        if new_cluster_name:
            return new_cluster_name
        return default_cluster_name

    @field_validator("type")
    @classmethod
    def switch_type(
        cls, type: DatabaseTypeEnum, all_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> DatabaseTypeEnum:
        """
        Подменяет тип движка из .env файла, если включена опция ENABLE_SWITCH_HOST.
        Ищет в .env файле переменные "DB_{tenant}_{name}_ENGINE_TYPE" и заменяет соответствующие значения модели Database.
        """
        if not settings.ENABLE_SWITCH_HOST or not all_fields.data.get("tenant_id"):
            return type
        tenant = all_fields.data.get("tenant_id")
        name = all_fields.data.get("name")
        engine_type_env = f"DB_{tenant}_{name}_ENGINE_TYPE".upper()
        new_type = getattr(settings, engine_type_env, None)
        return new_type or type

    @field_validator("db_name")
    @classmethod
    def db_name_validator(
        cls, db_name: Optional[str], all_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[str]:
        """
        Проверяет тип базы данных. Выдает ошибку, если db_name не задано для PostgreSQL и GreenPlum.
        Также подменяет имя бд из .env, если там есть DB_{tenant}_{name}_NAME.
        """
        new_db_name = None
        if settings.ENABLE_SWITCH_HOST and all_fields.data.get("tenant_id"):
            tenant = all_fields.data.get("tenant_id")
            name = all_fields.data.get("name")
            db_name_env = f"DB_{tenant}_{name}_NAME".upper()
            new_db_name = getattr(settings, db_name_env, None)
        db_name = new_db_name or db_name
        if not db_name and all_fields.data.get("type") != DatabaseTypeEnum.CLICKHOUSE:
            raise ValueError("The db_name cannot be null for PostgreSQL or GreenPlum")
        return db_name


class DatabaseEditRequest(BaseModel):
    """Схема базы данных для обновления patch запросом."""

    type: Optional[DatabaseTypeEnum] = Field(
        description=models_limitations["database"]["type"]["description"],
        max_length=models_limitations["database"]["type"]["max_length"],
        default=None,
    )
    default_cluster_name: Optional[str] = Field(
        default=None,
        description=models_limitations["database"]["default_cluster_name"]["description"],
        min_length=models_limitations["database"]["default_cluster_name"]["min_length"],
        max_length=models_limitations["database"]["default_cluster_name"]["max_length"],
        pattern=models_limitations["database"]["default_cluster_name"]["pattern"],
        serialization_alias=models_limitations["database"]["default_cluster_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["database"]["default_cluster_name"]["validation_alias"][0],
            models_limitations["database"]["default_cluster_name"]["validation_alias"][1],
        ),
    )
    db_name: Optional[str] = Field(
        default=None,
        description=models_limitations["database"]["db_name"]["description"],
        min_length=models_limitations["database"]["db_name"]["min_length"],
        max_length=models_limitations["database"]["db_name"]["max_length"],
        pattern=models_limitations["database"]["db_name"]["pattern"],
        serialization_alias=models_limitations["database"]["db_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["database"]["db_name"]["validation_alias"][0],
            models_limitations["database"]["db_name"]["validation_alias"][1],
        ),
    )
    connections: Optional[list[Connection]] = Field(
        description=models_limitations["database"]["connections"]["description"],
        default=None,
        max_length=models_limitations["database"]["connections"]["max_length"],
    )
    labels: Optional[list[Label]] = Field(
        description=models_limitations["database"]["labels"]["description"],
        default=None,
        max_length=models_limitations["database"]["labels"]["max_length"],
    )


class DatabaseCreateRequest(BaseModel):
    """Схема базы данных для создания post запросом."""

    name: str = Field(
        description=models_limitations["object_name_32"]["description"],
        min_length=models_limitations["object_name_32"]["min_length"],
        max_length=models_limitations["object_name_32"]["max_length"],
        pattern=models_limitations["object_name_32"]["pattern"],
    )
    default_cluster_name: Optional[str] = Field(
        default=None,
        description=models_limitations["database"]["default_cluster_name"]["description"],
        pattern=models_limitations["database"]["default_cluster_name"]["pattern"],
        min_length=models_limitations["database"]["default_cluster_name"]["min_length"],
        max_length=models_limitations["database"]["default_cluster_name"]["max_length"],
        serialization_alias=models_limitations["database"]["default_cluster_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["database"]["default_cluster_name"]["validation_alias"][0],
            models_limitations["database"]["default_cluster_name"]["validation_alias"][1],
        ),
    )
    type: DatabaseTypeEnum = Field(
        description=models_limitations["database"]["type"]["description"],
        max_length=models_limitations["database"]["type"]["max_length"],
    )
    db_name: Optional[str] = Field(
        default=None,
        validate_default=True,
        description=models_limitations["database"]["db_name"]["description"],
        min_length=models_limitations["database"]["db_name"]["min_length"],
        max_length=models_limitations["database"]["db_name"]["max_length"],
        pattern=models_limitations["database"]["db_name"]["pattern"],
        serialization_alias=models_limitations["database"]["db_name"]["serialization_alias"],
        validation_alias=AliasChoices(
            models_limitations["database"]["db_name"]["validation_alias"][0],
            models_limitations["database"]["db_name"]["validation_alias"][1],
        ),
    )
    connections: list[Connection] = Field(
        description=models_limitations["database"]["connections"]["description"],
        max_length=models_limitations["database"]["connections"]["max_length"],
    )
    labels: list[Label] = Field(
        description=models_limitations["database"]["labels"]["description"],
        default=[],
        max_length=models_limitations["database"]["labels"]["max_length"],
    )

    @field_validator("db_name")
    @classmethod
    def db_name_validator(
        cls, db_name: Optional[str], all_fields: core_schema.ValidationInfo, **kwargs: dict
    ) -> Optional[str]:
        """
        Проверяет тип базы данных. Выдает ошибку, если db_name не задано для PostgreSQL и GreenPlum.
        """
        if not db_name and all_fields.data.get("type") != DatabaseTypeEnum.CLICKHOUSE:
            raise ValueError("The db_name cannot be null for PostgreSQL or GreenPlum")
        return db_name
