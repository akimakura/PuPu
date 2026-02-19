from numpy import nan
from pandas import NA

from src.models.any_field import AnyFieldTypeEnum
from src.models.database import DatabaseTypeEnum
from src.models.dimension import DimensionTypeEnum, TextEnum
from src.models.label import LabelType, Language
from src.models.measure import MeasureTypeEnum
from src.models.request_params import (
    DataStorageFieldsFileColumnEnum,
    DataStorageFileColumnEnum,
    DimensionAttributesFileColumnEnum,
    DimensionsFileColumnEnum,
)

PV_DICTIONARIES = "PV_DICTIONARIES"

DATA_TYPES: dict = {
    DatabaseTypeEnum.CLICKHOUSE: {
        MeasureTypeEnum.FLOAT: "Float32",
        MeasureTypeEnum.INTEGER: "Int32",
        MeasureTypeEnum.DECIMAL: "Decimal",
        DimensionTypeEnum.STRING: "String",
        DimensionTypeEnum.DATE: "Date32",
        DimensionTypeEnum.TIME: "DateTime64",
        DimensionTypeEnum.DATETIME: "DateTime64",
        DimensionTypeEnum.TIMESTAMP: "DateTime64",
        DimensionTypeEnum.BOOLEAN: "Bool",
        DimensionTypeEnum.ARRAY_STRING: "Array(String)",
        DimensionTypeEnum.ARRAY_INTEGER: "Array(Int32)",
        DimensionTypeEnum.UUID: "UUID",
        AnyFieldTypeEnum.JSON: "String",
    },
    DatabaseTypeEnum.GREENPLUM: {
        MeasureTypeEnum.FLOAT: "float4",
        MeasureTypeEnum.INTEGER: "int4",
        MeasureTypeEnum.DECIMAL: "numeric",
        DimensionTypeEnum.STRING: "text",
        DimensionTypeEnum.DATE: "date",
        DimensionTypeEnum.TIME: "time",
        DimensionTypeEnum.DATETIME: "timestamp",
        DimensionTypeEnum.TIMESTAMP: "timestamptz",
        DimensionTypeEnum.ARRAY_INTEGER: "integer[]",
        DimensionTypeEnum.BOOLEAN: "bool",
        DimensionTypeEnum.UUID: "uuid",
        AnyFieldTypeEnum.JSON: "JSONB",
    },
    DatabaseTypeEnum.POSTGRESQL: {
        MeasureTypeEnum.FLOAT: "float4",
        MeasureTypeEnum.INTEGER: "int4",
        MeasureTypeEnum.DECIMAL: "numeric",
        DimensionTypeEnum.STRING: "text",
        DimensionTypeEnum.DATE: "date",
        DimensionTypeEnum.TIME: "time",
        DimensionTypeEnum.DATETIME: "timestamp",
        DimensionTypeEnum.TIMESTAMP: "timestamptz",
        DimensionTypeEnum.ARRAY_INTEGER: "integer[]",
        DimensionTypeEnum.BOOLEAN: "bool",
        DimensionTypeEnum.UUID: "uuid",
        AnyFieldTypeEnum.JSON: "JSONB",
    },
    PV_DICTIONARIES: {
        MeasureTypeEnum.FLOAT: "Double",
        MeasureTypeEnum.INTEGER: "Long",
        MeasureTypeEnum.DECIMAL: "BigDecimal",
        DimensionTypeEnum.STRING: "String",
        DimensionTypeEnum.DATE: "Date",
        DimensionTypeEnum.TIME: "String",
        DimensionTypeEnum.DATETIME: "String",
        DimensionTypeEnum.TIMESTAMP: "String",
        DimensionTypeEnum.ARRAY_INTEGER: "String",
        DimensionTypeEnum.BOOLEAN: "Boolean",
        DimensionTypeEnum.UUID: "String",
        DimensionTypeEnum.ARRAY_STRING: "String",
        AnyFieldTypeEnum.JSON: "String",
    },
}


DEFAULT_TYPE_VALUES: dict = {
    DatabaseTypeEnum.CLICKHOUSE: {
        MeasureTypeEnum.FLOAT: 0,
        MeasureTypeEnum.INTEGER: 0,
        MeasureTypeEnum.DECIMAL: 0,
        DimensionTypeEnum.STRING: "''",
        DimensionTypeEnum.DATE: "'1900-01-01'",
        DimensionTypeEnum.TIME: "'1900-01-01 00:00:00'",
        DimensionTypeEnum.DATETIME: "'1900-01-01 00:00:00'",
        DimensionTypeEnum.TIMESTAMP: "now()",
        DimensionTypeEnum.BOOLEAN: "false",
        DimensionTypeEnum.ARRAY_STRING: "'[]'",
        DimensionTypeEnum.ARRAY_INTEGER: "'[]'",
        DimensionTypeEnum.UUID: "generateUUIDv4()",
        AnyFieldTypeEnum.JSON: "''",
    },
    DatabaseTypeEnum.GREENPLUM: {
        MeasureTypeEnum.FLOAT: 0,
        MeasureTypeEnum.INTEGER: 0,
        MeasureTypeEnum.DECIMAL: 0,
        DimensionTypeEnum.STRING: "''",
        DimensionTypeEnum.DATE: "'1900-01-01'",
        DimensionTypeEnum.TIME: "'00:00:00'",
        DimensionTypeEnum.DATETIME: "'1900-01-01 00:00:00'",
        DimensionTypeEnum.TIMESTAMP: "now()",
        DimensionTypeEnum.BOOLEAN: "false",
        DimensionTypeEnum.ARRAY_STRING: "''",
        DimensionTypeEnum.ARRAY_INTEGER: "''",
        DimensionTypeEnum.UUID: "gen_random_uuid()",
        AnyFieldTypeEnum.JSON: "'{}'::jsonb",
    },
    DatabaseTypeEnum.POSTGRESQL: {
        MeasureTypeEnum.FLOAT: 0,
        MeasureTypeEnum.INTEGER: 0,
        MeasureTypeEnum.DECIMAL: 0,
        DimensionTypeEnum.STRING: "''",
        DimensionTypeEnum.DATE: "'1900-01-01'",
        DimensionTypeEnum.TIME: "'00:00:00'",
        DimensionTypeEnum.DATETIME: "'1900-01-01 00:00:00'",
        DimensionTypeEnum.TIMESTAMP: "now()",
        DimensionTypeEnum.BOOLEAN: "false",
        DimensionTypeEnum.ARRAY_STRING: "''",
        DimensionTypeEnum.ARRAY_INTEGER: "'{}'",
        DimensionTypeEnum.UUID: "gen_random_uuid()",
        AnyFieldTypeEnum.JSON: "'{}'::jsonb",
    },
    PV_DICTIONARIES: {
        MeasureTypeEnum.FLOAT: 0,
        MeasureTypeEnum.INTEGER: 0,
        MeasureTypeEnum.DECIMAL: 0,
        DimensionTypeEnum.STRING: "",
        DimensionTypeEnum.DATE: "1900-01-01",
        DimensionTypeEnum.TIME: "00:00:00",
        DimensionTypeEnum.DATETIME: "1900-01-01 00:00:00",
        DimensionTypeEnum.TIMESTAMP: "1900-01-01 00:00:00",
        DimensionTypeEnum.BOOLEAN: "false",
        DimensionTypeEnum.ARRAY_STRING: "",
        DimensionTypeEnum.ARRAY_INTEGER: "",
        DimensionTypeEnum.UUID: "''",
        AnyFieldTypeEnum.JSON: "''",
    },
}


TYPE: str = "TYPE"
LANGUAGE_FIELD = "language"
DATEFROM = "datefrom"
DATETO = "dateto"
LABELS_DATE = {
    DATETO: [{"language": Language.RU, "type": LabelType.SHORT, "text": "ДатаПо"}],
    DATEFROM: [{"language": Language.RU, "type": LabelType.SHORT, "text": "ДатаС"}],
}
TEXT_TO_LENGTH: dict[TextEnum | LabelType, int] = {
    TextEnum.SHORT: 20,
    TextEnum.MEDIUM: 40,
    TextEnum.LONG: 1333,
}

LENGTH_LANGUAGE_FIELD = 5
LENGTH_DATE_FIELD = 8
NOT_KEYS = {LANGUAGE_FIELD, DATEFROM, DATETO}
DEFAULT_DATE_FROM = "1900-01-01"
DEFAULT_DATE_TO = "2299-12-31"
DIMENSIONS_FILE_COLUMNS = (
    DimensionsFileColumnEnum.NAME,
    DimensionsFileColumnEnum.SHORT_LABEL,
    DimensionsFileColumnEnum.LONG_LABEL,
    DimensionsFileColumnEnum.REF,
    DimensionsFileColumnEnum.DATA_TYPE,
    DimensionsFileColumnEnum.LENGTH,
    DimensionsFileColumnEnum.VIRTUAL,
    DimensionsFileColumnEnum.SHORT_TEXT,
    DimensionsFileColumnEnum.MEDIUM_TEXT,
    DimensionsFileColumnEnum.LONG_TEXT,
    DimensionsFileColumnEnum.TEXT_TIME_DEPENDENCY,
    DimensionsFileColumnEnum.TEXT_LANG_DEPENDENCY,
    DimensionsFileColumnEnum.AUTH_RELEVANT,
    DimensionsFileColumnEnum.CASE_SENSITIVE,
)
DIMENSION_ATTRIBUTES_FILE_COLUMNS = (
    DimensionAttributesFileColumnEnum.DIMENSION_NAME,
    DimensionAttributesFileColumnEnum.NAME,
    DimensionAttributesFileColumnEnum.TIME_DEPENDENCY,
    DimensionAttributesFileColumnEnum.SHORT_LABEL,
    DimensionAttributesFileColumnEnum.LONG_LABEL,
    DimensionAttributesFileColumnEnum.SEMANTIC_TYPE,
    DimensionAttributesFileColumnEnum.REF,
    DimensionAttributesFileColumnEnum.DATA_TYPE,
    DimensionAttributesFileColumnEnum.LENGTH,
    DimensionAttributesFileColumnEnum.SCALE,
    DimensionAttributesFileColumnEnum.AGGREGATION_TYPE,
)

DATA_STORAGE_FILE_COLUMNS = (
    DataStorageFileColumnEnum.NAME,
    DataStorageFileColumnEnum.SHORT_LABEL,
    DataStorageFileColumnEnum.LONG_LABEL,
    DataStorageFileColumnEnum.PLAN,
    DataStorageFileColumnEnum.TYPE,
)

DATA_STORAGE_FIELDS_FILE_COLUMN = (
    DataStorageFieldsFileColumnEnum.DATA_STORAGE_NAME,
    DataStorageFieldsFileColumnEnum.FIELD_NAME,
    DataStorageFieldsFileColumnEnum.KEY,
    DataStorageFieldsFileColumnEnum.SHARDING_KEY,
    DataStorageFieldsFileColumnEnum.SHORT_LABEL,
    DataStorageFieldsFileColumnEnum.LONG_LABEL,
    DataStorageFieldsFileColumnEnum.SEMANTIC_TYPE,
    DataStorageFieldsFileColumnEnum.REF,
    DataStorageFieldsFileColumnEnum.DATA_TYPE,
    DataStorageFieldsFileColumnEnum.LENGTH,
    DataStorageFieldsFileColumnEnum.SCALE,
    DataStorageFieldsFileColumnEnum.AGGREGATION_TYPE,
)

DIMENSIONS_FILE_COLUMNS_TYPE = {
    DimensionsFileColumnEnum.NAME: "str",
    DimensionsFileColumnEnum.SHORT_LABEL: "str",
    DimensionsFileColumnEnum.LONG_LABEL: "str",
    DimensionsFileColumnEnum.REF: "str",
    DimensionsFileColumnEnum.DATA_TYPE: "str",
    DimensionsFileColumnEnum.LENGTH: "Int64",
    DimensionsFileColumnEnum.VIRTUAL: "bool",
    DimensionsFileColumnEnum.SHORT_TEXT: "bool",
    DimensionsFileColumnEnum.MEDIUM_TEXT: "bool",
    DimensionsFileColumnEnum.LONG_TEXT: "bool",
    DimensionsFileColumnEnum.TEXT_TIME_DEPENDENCY: "bool",
    DimensionsFileColumnEnum.TEXT_LANG_DEPENDENCY: "bool",
    DimensionsFileColumnEnum.AUTH_RELEVANT: "bool",
    DimensionsFileColumnEnum.CASE_SENSITIVE: "bool",
}

DIMENSIONS_ATTRIBUTES_FILE_COLUMNS_TYPE = {
    DimensionAttributesFileColumnEnum.DIMENSION_NAME: "str",
    DimensionAttributesFileColumnEnum.NAME: "str",
    DimensionAttributesFileColumnEnum.TIME_DEPENDENCY: "bool",
    DimensionAttributesFileColumnEnum.SHORT_LABEL: "str",
    DimensionAttributesFileColumnEnum.LONG_LABEL: "str",
    DimensionAttributesFileColumnEnum.SEMANTIC_TYPE: "str",
    DimensionAttributesFileColumnEnum.REF: "str",
    DimensionAttributesFileColumnEnum.DATA_TYPE: "str",
    DimensionAttributesFileColumnEnum.LENGTH: "Int64",
    DimensionAttributesFileColumnEnum.SCALE: "Int64",
    DimensionAttributesFileColumnEnum.AGGREGATION_TYPE: "str",
}


DATA_STORAGE_FILE_COLUMNS_TYPE = {
    DataStorageFileColumnEnum.NAME: "str",
    DataStorageFileColumnEnum.SHORT_LABEL: "str",
    DataStorageFileColumnEnum.LONG_LABEL: "str",
    DataStorageFileColumnEnum.PLAN: "bool",
    DataStorageFileColumnEnum.TYPE: "str",
}

DATA_STORAGE_FIELDS_FILE_COLUMNS_TYPE = {
    DataStorageFieldsFileColumnEnum.DATA_STORAGE_NAME: "str",
    DataStorageFieldsFileColumnEnum.FIELD_NAME: "str",
    DataStorageFieldsFileColumnEnum.KEY: "bool",
    DataStorageFieldsFileColumnEnum.SHARDING_KEY: "bool",
    DataStorageFieldsFileColumnEnum.SHORT_LABEL: "str",
    DataStorageFieldsFileColumnEnum.LONG_LABEL: "str",
    DataStorageFieldsFileColumnEnum.SEMANTIC_TYPE: "str",
    DataStorageFieldsFileColumnEnum.REF: "str",
    DataStorageFieldsFileColumnEnum.DATA_TYPE: "str",
    DataStorageFieldsFileColumnEnum.LENGTH: "Int64",
    DataStorageFieldsFileColumnEnum.SCALE: "Int64",
    DataStorageFieldsFileColumnEnum.AGGREGATION_TYPE: "str",
}


REPLACE_VALUE_DATAFRAME_DICT = dict.fromkeys((nan, NA, "", "nan", "None"), None)
DATASOURCE_FIELD = "data_source"
