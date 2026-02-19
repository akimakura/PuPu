from enum import StrEnum


class CacheNamespaceEnum(StrEnum):
    COMPOSITE = "composite"
    DATASTORAGE = "datastorage"
    DATABASE = "database"
    DIMENSION = "dimension"
    HIERARCHY = "hierarchy"
    MEASURE = "measure"
    MODEL = "model"
    TENANT = "tenant"
