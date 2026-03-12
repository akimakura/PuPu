"""Модели SQLAlchemy для описания поля DSO типа Measure (показатель)."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import FieldTypeMixin, LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.composite import CompositeField
    from src.db.data_storage import DataStorageField
    from src.db.dimension import DimensionAttribute


class AnyField(FieldTypeMixin, Versioned, Base):
    """Поле без справочника"""

    __tablename__ = "any_field"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"dimension_attributes", "data_storage_fields", "composite_fields", "labels"},
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    scale: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    aggregation_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    precision: Mapped[int] = mapped_column(Integer, nullable=True, default=None)
    allow_null_values: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Relationships
    labels: Mapped[list["AnyFieldLabel"]] = relationship(
        back_populates="any_field", lazy="selectin", cascade="all,delete"
    )
    dimension_attributes: Mapped[list["DimensionAttribute"]] = relationship(back_populates="any_field_attribute")
    data_storage_fields: Mapped[list["DataStorageField"]] = relationship(back_populates="any_field")
    composite_fields: Mapped[list["CompositeField"]] = relationship(back_populates="any_field")


class AnyFieldLabel(LabelMixin, Versioned, Base):
    """Описание AnyField."""

    __tablename__ = "any_field_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "any_field",
        },
    }
    # Fields
    any_field_id: Mapped[int] = mapped_column(ForeignKey("any_field.id"), nullable=False)

    # Relationships
    any_field: Mapped["AnyField"] = relationship(
        back_populates="labels",
        foreign_keys=[any_field_id],
    )
