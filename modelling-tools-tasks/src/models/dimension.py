from enum import StrEnum


class TechDimensionEnum(StrEnum):
    """Виды технических измерений."""

    TIMESTAMP = "timestamp"
    DELETED = "deleted"
    IS_ACTIVE_DIMENSION = "is_active"
