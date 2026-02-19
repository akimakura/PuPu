"""Модели SQLAlchemy для описания баз данных и напрямую связанных с ними объектов."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.model import Model
    from src.db.tenant import Tenant


class Database(Base, Versioned):
    """База данных."""

    __tablename__ = "database"  # type: ignore
    __table_args__ = (UniqueConstraint("name", "tenant_id", name="unique_tenant_database_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {"models", "tenant", "connections", "labels"},
        "check_modified_fields": {"db_name", "default_cluster_name", "type"},
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)
    db_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    default_cluster_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Relationships
    tenant: Mapped["Tenant"] = relationship(back_populates="databases", foreign_keys=[tenant_id])
    labels: Mapped[list["DatabaseLabel"]] = relationship(
        back_populates="database", lazy="selectin", cascade="all,delete-orphan"
    )
    connections: Mapped[list["Connection"]] = relationship(
        back_populates="database", lazy="selectin", cascade="all, delete-orphan"
    )
    models: Mapped[list["Model"]] = relationship(back_populates="database")


class Connection(Base, Versioned):
    """Строка подключения к базе данных."""

    __tablename__ = "connection"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"database"},
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    host: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    database_id: Mapped[int] = mapped_column(ForeignKey("database.id"), nullable=False)

    # Relationships
    database: Mapped["Database"] = relationship(back_populates="connections", foreign_keys=[database_id])
    ports: Mapped[list["Port"]] = relationship(back_populates="connection", lazy="selectin", cascade="all,delete")


class Port(Base, Versioned):
    """Порт подключения к базе данных."""

    __tablename__ = "port"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"connection"},
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    protocol: Mapped[str] = mapped_column(String, nullable=False)
    sql_dialect: Mapped[str] = mapped_column(String, nullable=False)
    secured: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    connection_id: Mapped[int] = mapped_column(ForeignKey("connection.id"), nullable=False)

    # Relationships
    connection: Mapped["Connection"] = relationship(back_populates="ports", foreign_keys=[connection_id])


class DatabaseLabel(Base, LabelMixin, Versioned):
    """Текстовое описание базы данных."""

    __tablename__ = "database_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"database"},
    }
    # Fields
    database_id: Mapped[int] = mapped_column(ForeignKey("database.id"), nullable=False)

    # Relationships
    database: Mapped["Database"] = relationship(back_populates="labels", foreign_keys=[database_id])
