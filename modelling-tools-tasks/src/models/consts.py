from src.models.database import DatabaseTypeEnum
from src.models.types import SemanticDataTypeEnum

PV_DICTIONARIES = "PV_DICTIONARIES"

DATA_TYPES: dict[DatabaseTypeEnum | str, dict[SemanticDataTypeEnum, str]] = {
    DatabaseTypeEnum.CLICKHOUSE: {
        SemanticDataTypeEnum.FLOAT: "Float32",
        SemanticDataTypeEnum.INTEGER: "Int32",
        SemanticDataTypeEnum.DECIMAL: "Decimal",
        SemanticDataTypeEnum.STRING: "String",
        SemanticDataTypeEnum.DATE: "Date32",
        SemanticDataTypeEnum.TIME: "DateTime64",
        SemanticDataTypeEnum.DATETIME: "DateTime64",
        SemanticDataTypeEnum.TIMESTAMP: "DateTime64",
        SemanticDataTypeEnum.BOOLEAN: "Bool",
        SemanticDataTypeEnum.ARRAY_STRING: "Array(String)",
        SemanticDataTypeEnum.UUID: "UUID",
    },
    DatabaseTypeEnum.GREENPLUM: {
        SemanticDataTypeEnum.FLOAT: "float4",
        SemanticDataTypeEnum.INTEGER: "int4",
        SemanticDataTypeEnum.DECIMAL: "numeric",
        SemanticDataTypeEnum.STRING: "text",
        SemanticDataTypeEnum.DATE: "date",
        SemanticDataTypeEnum.TIME: "time",
        SemanticDataTypeEnum.DATETIME: "timestamp",
        SemanticDataTypeEnum.TIMESTAMP: "timestamptz",
        SemanticDataTypeEnum.BOOLEAN: "bool",
        SemanticDataTypeEnum.UUID: "uuid",
        SemanticDataTypeEnum.ARRAY_STRING: "text",
    },
    DatabaseTypeEnum.POSTGRESQL: {
        SemanticDataTypeEnum.FLOAT: "float4",
        SemanticDataTypeEnum.INTEGER: "int4",
        SemanticDataTypeEnum.DECIMAL: "numeric",
        SemanticDataTypeEnum.STRING: "text",
        SemanticDataTypeEnum.DATE: "date",
        SemanticDataTypeEnum.TIME: "time",
        SemanticDataTypeEnum.DATETIME: "timestamp",
        SemanticDataTypeEnum.TIMESTAMP: "timestamptz",
        SemanticDataTypeEnum.BOOLEAN: "bool",
        SemanticDataTypeEnum.UUID: "uuid",
        SemanticDataTypeEnum.ARRAY_STRING: "text",
    },
    PV_DICTIONARIES: {
        SemanticDataTypeEnum.FLOAT: "Double",
        SemanticDataTypeEnum.INTEGER: "Long",
        SemanticDataTypeEnum.DECIMAL: "BigDecimal",
        SemanticDataTypeEnum.STRING: "String",
        SemanticDataTypeEnum.DATE: "Date",
        SemanticDataTypeEnum.TIME: "String",
        SemanticDataTypeEnum.DATETIME: "String",
        SemanticDataTypeEnum.TIMESTAMP: "String",
        SemanticDataTypeEnum.BOOLEAN: "Boolean",
        SemanticDataTypeEnum.UUID: "String",
        SemanticDataTypeEnum.ARRAY_STRING: "String",
    },
}


DEFAULT_TYPE_VALUES: dict[DatabaseTypeEnum | str, dict[str, str | int]] = {
    DatabaseTypeEnum.CLICKHOUSE: {
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.FLOAT]: 0,
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.INTEGER]: 0,
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.DECIMAL]: 0,
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.STRING]: "''",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.DATE]: "'1900-01-01'",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.TIME]: "'1900-01-01 00:00:00'",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.DATETIME]: "'1900-01-01 00:00:00'",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.TIMESTAMP]: "now()",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.BOOLEAN]: "false",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.ARRAY_STRING]: "''",
        DATA_TYPES[DatabaseTypeEnum.CLICKHOUSE][SemanticDataTypeEnum.UUID]: "generateUUIDv4()",
    },
    DatabaseTypeEnum.GREENPLUM: {
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.FLOAT]: 0,
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.INTEGER]: 0,
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.DECIMAL]: 0,
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.STRING]: "''",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.DATE]: "'1900-01-01'",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.TIME]: "'00:00:00'",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.DATETIME]: "'1900-01-01 00:00:00'",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.TIMESTAMP]: "now()",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.BOOLEAN]: "false",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.ARRAY_STRING]: "''",
        DATA_TYPES[DatabaseTypeEnum.GREENPLUM][SemanticDataTypeEnum.UUID]: "gen_random_uuid()",
    },
    DatabaseTypeEnum.POSTGRESQL: {
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.FLOAT]: 0,
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.INTEGER]: 0,
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.DECIMAL]: 0,
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.STRING]: "''",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.DATE]: "'1900-01-01'",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.TIME]: "'00:00:00'",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.DATETIME]: "'1900-01-01 00:00:00'",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.TIMESTAMP]: "now()",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.BOOLEAN]: "false",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.ARRAY_STRING]: "''",
        DATA_TYPES[DatabaseTypeEnum.POSTGRESQL][SemanticDataTypeEnum.UUID]: "gen_random_uuid()",
    },
    PV_DICTIONARIES: {
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.FLOAT]: 0,
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.INTEGER]: 0,
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.DECIMAL]: 0,
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.STRING]: "",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.DATE]: "1900-01-01",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.TIME]: "00:00:00",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.DATETIME]: "1900-01-01 00:00:00",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.TIMESTAMP]: "1900-01-01 00:00:00",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.BOOLEAN]: "false",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.ARRAY_STRING]: "",
        DATA_TYPES[PV_DICTIONARIES][SemanticDataTypeEnum.UUID]: "''",
    },
}

TYPE: str = "TYPE"
LANGUAGE_FIELD = "language"
DATEFROM = "datefrom"
DATETO = "dateto"
DEFAULT_DATE_FROM = "1900-01-01"
DEFAULT_DATE_TO = "2299-12-31"
TIMESTAMP = "timestamp"
DATASOURCE_FIELD = "data_source"
