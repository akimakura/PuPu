"""Модели SQLAlchemy для описания DSO и напрямую связанных с ним объектов."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import FieldMixin, LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.any_field import AnyField
    from src.db.database_object import DatabaseObject
    from src.db.dimension import Dimension
    from src.db.measure import Measure
    from src.db.model import Model
    from src.db.tenant import Tenant


class DataStorageModelRelation(Base, Versioned):
    """Связь между моделью и DataStorage."""

    __tablename__ = "data_storage_model_relation"  # type: ignore
    __table_args__ = (
        UniqueConstraint(
            "data_storage_id",
            "model_id",
            name="unique_model_id_data_storage_id_combination",
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_storage_id: Mapped[int] = mapped_column(ForeignKey("data_storage.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING", server_default=text("'PENDING'"))
    msg: Mapped[str] = mapped_column(String, nullable=True)
    is_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))


class DataStorage(Base, Versioned):
    """Абстракция над технической таблицей для хранения метаинформации."""

    __tablename__ = "data_storage"  # type: ignore
    __table_args__ = (UniqueConstraint("name", "tenant_id", name="unique_tenant_data_storage_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {
            "database_objects",
            "log_data_storage",
            "fields",
            "labels",
            "models",
            "tenant",
        },
        "check_modified_fields": {
            "planning_enabled",
            "type",
        },
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    information_category: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    planning_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)
    log_data_storage_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage.id", ondelete="RESTRICT"), nullable=True
    )
    sharding_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Relationships
    model_relations: Mapped[list["DataStorageModelRelation"]] = relationship(
        "DataStorageModelRelation", lazy="selectin", viewonly=True
    )
    models: Mapped[list["Model"]] = relationship("Model", secondary=DataStorageModelRelation.__table__, lazy="selectin")
    tenant: Mapped["Tenant"] = relationship(back_populates="data_storages", foreign_keys=[tenant_id])
    labels: Mapped[list["DataStorageLabel"]] = relationship(
        back_populates="data_storage", lazy="selectin", cascade="all, delete-orphan"
    )
    fields: Mapped[list["DataStorageField"]] = relationship(
        back_populates="data_storage",
        lazy="selectin",
        cascade="all, delete-orphan",
        order_by="DataStorageField.id",
    )
    database_objects: Mapped[list["DatabaseObject"]] = relationship(
        back_populates="data_storage", cascade="all, delete-orphan"
    )
    log_data_storage: Mapped[Optional["DataStorage"]] = relationship(
        "DataStorage",
        remote_side=[id],
        foreign_keys=[log_data_storage_id],
        cascade="all,delete-orphan",
        uselist=False,
        single_parent=True,
    )

    def __repr__(self) -> str:
        return f"{self.tenant_id}.{self.name}"


class DataStorageLabel(Base, LabelMixin, Versioned):
    """Текстовое описание DataStorage."""

    __tablename__ = "data_storage_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"data_storage"},
    }
    # Fields
    data_storage_id: Mapped[int] = mapped_column(ForeignKey("data_storage.id"), nullable=False)

    # Relationships
    data_storage: Mapped["DataStorage"] = relationship(back_populates="labels", foreign_keys=[data_storage_id])


class DataStorageField(Base, FieldMixin, Versioned):
    """Поля DataStorage."""

    __tablename__ = "data_storage_field"  # type: ignore
    __table_args__ = (UniqueConstraint("data_storage_id", "name", name="unique_data_storage_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {
            "dimension",
            "measure",
            "any_field",
            "labels",
            "data_storage",
        },
    }
    # Fields
    is_key: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    data_storage_id: Mapped[int] = mapped_column(ForeignKey("data_storage.id"), nullable=False)
    is_sharding_key: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    is_tech_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    allow_null_values_local: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Relationships
    data_storage: Mapped["DataStorage"] = relationship(
        back_populates="fields", foreign_keys=[data_storage_id], lazy="joined"
    )
    labels: Mapped[list["DataStorageFieldLabel"]] = relationship(
        back_populates="data_storage_field", lazy="selectin", cascade="all,delete-orphan"
    )
    dimension: Mapped[Optional["Dimension"]] = relationship(back_populates="data_storage_fields", lazy="selectin")
    measure: Mapped[Optional["Measure"]] = relationship(back_populates="data_storage_fields", lazy="selectin")
    any_field: Mapped[Optional["AnyField"]] = relationship(
        back_populates="data_storage_fields", lazy="selectin", cascade="all,delete"
    )


class DataStorageFieldLabel(Base, LabelMixin, Versioned):
    """Текстовое описание DataStorageField."""

    __tablename__ = "data_storage_field_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"data_storage_field"},
    }
    # Fields
    data_storage_field_id: Mapped[int] = mapped_column(ForeignKey("data_storage_field.id"), nullable=False)

    # Relationships
    data_storage_field: Mapped["DataStorageField"] = relationship(
        back_populates="labels", foreign_keys=[data_storage_field_id]
    )
