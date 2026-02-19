"""Модели SQLAlchemy для описания объектов базы данных."""

from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.composite import Composite
    from src.db.data_storage import DataStorage
    from src.db.model import Model


class DatabaseObjectModelRelation(Base, Versioned):
    """Связь между моделью и DatabaseObject."""

    __tablename__ = "database_object_model_relation"  # type: ignore
    __table_args__ = (
        UniqueConstraint("database_object_id", "model_id", name="unique_model_id_database_object_id_combination"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    database_object_id: Mapped[int] = mapped_column(ForeignKey("database_object.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING", server_default=text("'PENDING'"))
    msg: Mapped[str] = mapped_column(String, nullable=True)
    is_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))


class DatabaseObjectRelation(Base, Versioned):
    """
    Связь объекта Modeling Tool с найденным database_object.
    Здесь будет храниться мэппинг наших и собранных объектов.
    """

    __tablename__ = "database_object_relations"  # type: ignore
    if TYPE_CHECKING:
        version: Mapped[int]

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    semantic_object_type: Mapped[str] = mapped_column(String, nullable=False)
    semantic_object_id: Mapped[int] = mapped_column(Integer, nullable=False)
    semantic_object_version: Mapped[int] = mapped_column(Integer, nullable=False)
    database_object_id: Mapped[int] = mapped_column(
        ForeignKey("database_object.id", ondelete="CASCADE"), nullable=False
    )
    database_object_version: Mapped[int] = mapped_column(Integer, nullable=False)
    relation_type: Mapped[str | None] = mapped_column(
        String, nullable=True, default="PARENT", server_default=text("'PARENT'")
    )


class DatabaseObject(Base, Versioned):
    """
    Объект в базе данных.
    Например: таблица, представление.
    """

    __tablename__ = "database_object"  # type: ignore
    __versioned__ = {"not_versioned_fields": {"composite", "models", "data_storage", "specific_attributes"}}
    if TYPE_CHECKING:
        version: Mapped[int]

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    schema_name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name", ondelete="RESTRICT"), nullable=False)
    data_storage_id: Mapped[int] = mapped_column(ForeignKey("data_storage.id", ondelete="CASCADE"), nullable=True)
    composite_id: Mapped[int] = mapped_column(ForeignKey("composite.id", ondelete="CASCADE"), nullable=True)
    json_definition: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    specific_attributes: Mapped[list["DataBaseObjectSpecificAttribute"]] = relationship(
        "DataBaseObjectSpecificAttribute",
        cascade="all, delete-orphan",
        lazy="selectin",
    )
    # Relationships
    data_storage: Mapped["DataStorage"] = relationship(back_populates="database_objects")
    models: Mapped[list["Model"]] = relationship(
        "Model", secondary=DatabaseObjectModelRelation.__table__, lazy="selectin"
    )
    model_relations: Mapped[list["DatabaseObjectModelRelation"]] = relationship(
        "DatabaseObjectModelRelation", lazy="selectin", viewonly=True
    )
    composite: Mapped["Composite"] = relationship(back_populates="database_objects")


class DataBaseObjectSpecificAttribute(Base, Versioned):
    """
    Специфичный атрибут для объекта в базе данных.
    """

    __tablename__ = "database_object_specific_attribute"  # type: ignore
    __versioned__ = {"not_versioned_fields": set()}

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[str] = mapped_column(String, nullable=False)
    database_object_id: Mapped[int] = mapped_column(ForeignKey("database_object.id"), nullable=False)
