from enum import StrEnum


class DatabaseTypeEnum(StrEnum):
    CLICKHOUSE = "CLICKHOUSE"
    POSTGRESQL = "POSTGRESQL"
    GREENPLUM = "GREENPLUM"
