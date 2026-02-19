"""Модели SQLAlchemy для описания иерархий признака"""

import enum
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String
from sqlalchemy.orm import mapped_column, relationship, validates
from sqlalchemy.orm.base import Mapped

from src.db import Dimension, HierarchyBaseDimension
from src.db.dimension import PVDctionary
from src.db.meta import Base
from src.db.mixins import LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.model import Model


class HierarchyStructureType(enum.StrEnum):
    """Тип структуры иерархии"""

    MIXED = "MIXED"
    PARENT_CHILD = "PARENT_CHILD"
    FLAT = "FLAT"


class TimeDependencyType(enum.StrEnum):
    """Тип временной зависимости иерархии"""

    WHOLE = "WHOLE"
    NODE = "NODE"


class AggregationType(enum.StrEnum):
    """Тип агрегации данных в иерархии"""

    NONE = "NONE"
    SUM = "SUM"
    AVG = "AVG"


class HierarchyModelRelation(Base, Versioned):
    """Связь между иерархией и моделью"""

    __tablename__ = "hierarchy_model_relation"  # type: ignore

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hierarchy_id: Mapped[int] = mapped_column(Integer, ForeignKey("hierarchy_meta.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(Integer, ForeignKey("model.id"), nullable=False)
    is_owner: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class HierarchyMeta(Base, Versioned):
    """Мета-данные иерархии"""

    __tablename__ = "hierarchy_meta"  # type: ignore

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(32), nullable=False)
    pv_dictionary_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("pv_dictionary.id", ondelete="SET NULL"), nullable=True, default=None
    )
    default_expansion: Mapped[int] = mapped_column(default=3)

    structure_type: Mapped[str] = mapped_column(default=HierarchyStructureType.MIXED.value)
    time_dependency_type: Mapped[str] = mapped_column(nullable=True, default=None)
    aggregation_type: Mapped[str] = mapped_column(default=AggregationType.SUM.value)

    default_hierarchy: Mapped[bool] = mapped_column(default=False)
    is_time_dependent: Mapped[bool] = mapped_column(default=False)
    input_on_nodes: Mapped[bool] = mapped_column(default=False)
    is_versioned: Mapped[bool] = mapped_column(default=False)

    data_storage_versions: Mapped[str] = mapped_column(String(64), nullable=False)
    data_storage_text_versions: Mapped[str] = mapped_column(String(64), nullable=False)
    data_storage_nodes: Mapped[str] = mapped_column(String(64), nullable=False)
    data_storage_text_nodes: Mapped[str] = mapped_column(String(64), nullable=False)

    labels: Mapped[list["HierarchyLabel"]] = relationship(
        back_populates="hierarchy", lazy="selectin", cascade="all, delete-orphan"
    )
    models: Mapped[list["Model"]] = relationship("Model", secondary=HierarchyModelRelation.__table__, lazy="selectin")
    base_dimensions: Mapped[list["Dimension"]] = relationship(
        back_populates="hierarchies", secondary=HierarchyBaseDimension.__table__, lazy="selectin"
    )
    pv_dictionary: Mapped[Optional["PVDctionary"]] = relationship(
        "PVDctionary",
        foreign_keys=[pv_dictionary_id],
        lazy="joined",
        uselist=False,
    )

    @validates("default_expansion")
    def validate_default_expansion(self, key: Any, value: Optional[int]) -> int:
        if value is None:
            raise ValueError("Default expansion could be None")
        return value


class HierarchyLabel(LabelMixin, Base, Versioned):
    """Метка иерархии"""

    __tablename__ = "hierarchy_label"  # type: ignore

    hierarchy_id: Mapped[int] = mapped_column(Integer, ForeignKey("hierarchy_meta.id"), nullable=False)
    hierarchy: Mapped["HierarchyMeta"] = relationship(back_populates="labels", foreign_keys=[hierarchy_id])
