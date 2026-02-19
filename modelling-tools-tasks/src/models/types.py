from enum import StrEnum


class SemanticDataTypeEnum(StrEnum):
    FLOAT = "FLOAT"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    STRING = "STRING"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    ARRAY_STRING = "ARRAY_STRING"
    UUID = "UUID"
