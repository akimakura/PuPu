"""Модели SQLAlchemy для описания поля DSO типа Measure (показатель)."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import FieldTypeMixin, LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.composite import CompositeField
    from src.db.data_storage import DataStorageField
    from src.db.dimension import Dimension, DimensionAttribute
    from src.db.model import Model
    from src.db.tenant import Tenant


class MeasureModelRelation(Base, Versioned):
    """Связь между моделью и Dimension."""

    __tablename__ = "measure_model_relation"  # type: ignore
    __table_args__ = (UniqueConstraint("measure_id", "model_id", name="unique_model_id_measure_id_combination"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    measure_id: Mapped[int] = mapped_column(ForeignKey("measure.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="SUCCESS", server_default=text("'SUCCESS'"))
    msg: Mapped[str] = mapped_column(String, nullable=True)
    is_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))


class Measure(FieldTypeMixin, Versioned, Base):
    """Показатель (некоторое число)."""

    __tablename__ = "measure"  # type: ignore
    __table_args__ = (UniqueConstraint("name", "tenant_id", name="unique_tenant_measure_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {
            "models",
            "labels",
            "dimension",
            "filter",
            "dimension_attributes",
            "data_storage_fields",
            "composite_fields",
        },
        "check_modified_fields": {
            "type",
            "precision",
            "scale",
            "auth_relevant",
            "aggregation_type",
        },
    }
    # Fields
    scale: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    auth_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False)
    aggregation_type: Mapped[str] = mapped_column(String, nullable=False)
    dimension_id: Mapped[Optional[int]] = mapped_column(ForeignKey("dimension.id", ondelete="RESTRICT"), nullable=True)
    dimension_value: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)

    # Relationships
    models: Mapped[list["Model"]] = relationship("Model", secondary=MeasureModelRelation.__table__, lazy="selectin")
    model_relations: Mapped[list["MeasureModelRelation"]] = relationship(
        "MeasureModelRelation", lazy="selectin", viewonly=True
    )
    labels: Mapped[list["MeasureLabel"]] = relationship(
        back_populates="measure",
        lazy="selectin",
        cascade="all,delete-orphan",
    )
    dimension: Mapped[Optional["Dimension"]] = relationship(back_populates="measures", lazy="joined")
    tenant: Mapped["Tenant"] = relationship(back_populates="measures", foreign_keys=[tenant_id])
    filter: Mapped[list["DimensionFilter"]] = relationship(
        back_populates="measure",
        lazy="selectin",
        cascade="all,delete-orphan",
    )
    dimension_attributes: Mapped[list["DimensionAttribute"]] = relationship(
        back_populates="measure_attribute", viewonly=True
    )
    data_storage_fields: Mapped[list["DataStorageField"]] = relationship(back_populates="measure", passive_deletes=True)
    composite_fields: Mapped[list["CompositeField"]] = relationship(back_populates="measure", passive_deletes=True)


class MeasureLabel(LabelMixin, Versioned, Base):
    """Описание показателя."""

    __tablename__ = "measure_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"measure"},
    }
    # Fields
    measure_id: Mapped[int] = mapped_column(ForeignKey("measure.id"), nullable=False)

    # Relationships
    measure: Mapped["Measure"] = relationship(
        back_populates="labels",
        foreign_keys=[measure_id],
    )


class DimensionFilter(Base, Versioned):

    __tablename__ = "dimension_filter"  # type: ignore
    __versioned__ = {"not_versioned_fields": {"measure", "dimension"}}
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dimension_id: Mapped[int] = mapped_column(ForeignKey("dimension.id"), nullable=False)
    dimension_value: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    measure_id: Mapped[int] = mapped_column(ForeignKey("measure.id"), nullable=False)

    # Relationships
    measure: Mapped["Measure"] = relationship(back_populates="filter", foreign_keys=[measure_id])
    dimension: Mapped["Dimension"] = relationship(back_populates="filter", foreign_keys=[dimension_id], lazy="joined")
