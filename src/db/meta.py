"""
Формирование Base для работы с БД.
"""

from typing import Any, Sequence

from sqlalchemy.orm import DeclarativeBase, declared_attr
from sqlalchemy.schema import MetaData

from src.config import settings

# Recommended naming convention used by Alembic, as various different database
# providers will autogenerate vastly different names making migrations more
# difficult. See: https://alembic.sqlalchemy.org/en/latest/naming.html
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Базовый класс метаданных."""

    metadata = MetaData(schema=settings.DB_SCHEMA, naming_convention=NAMING_CONVENTION)

    __table_args__: Sequence[Any] = ({"schema": settings.DB_SCHEMA},)

    @declared_attr.directive
    def __tablename__(cls) -> str:
        """Дефолтное название таблицы совпадает с названием класса."""
        return cls.__name__.lower()


metadata = Base.metadata
