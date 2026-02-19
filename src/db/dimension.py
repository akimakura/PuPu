"""Модели SQLAlchemy для описания поля DSO типа Characteristic (признак)."""

from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import FieldTypeMixin, LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.any_field import AnyField
    from src.db.composite import CompositeField
    from src.db.data_storage import DataStorage, DataStorageField
    from src.db.hierarchy import HierarchyMeta
    from src.db.measure import DimensionFilter, Measure
    from src.db.model import Model
    from src.db.tenant import Tenant


class DimensionModelRelation(Base, Versioned):
    """
    Связь между моделью и измерением (Dimension).

    Представляет таблицу отношений между измерениями и моделями.
    Каждое измерение может принадлежать одной или нескольким моделям.
    """

    __tablename__ = "dimension_model_relation"  # type: ignore
    __table_args__ = (UniqueConstraint("dimension_id", "model_id", name="unique_model_id_dimension_id_combination"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dimension_id: Mapped[int] = mapped_column(ForeignKey("dimension.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING", server_default=text("'PENDING'"))
    msg: Mapped[str] = mapped_column(String, nullable=True)
    is_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))


class HierarchyBaseDimension(Base, Versioned):
    """
    Таблица, хранящая отношения между иерархиями и базовыми измерениями.

    Каждая запись в таблице определяет, какое измерение является основным для определенной иерархии.
    """

    __tablename__ = "hierarchy_base_dimension"  # type: ignore
    __table_args__ = (
        UniqueConstraint("hierarchy_id", "dimension_id", name="unique_hierarchy_id_dimension_id_combination"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hierarchy_id: Mapped[int] = mapped_column(Integer, ForeignKey("hierarchy_meta.id"), nullable=False)
    dimension_id: Mapped[int] = mapped_column(Integer, ForeignKey("dimension.id"), nullable=False)
    is_base: Mapped[bool] = mapped_column(Boolean, nullable=False)


class Dimension(FieldTypeMixin, Base, Versioned):
    """Измерения."""

    __tablename__ = "dimension"  # type: ignore
    __table_args__ = (UniqueConstraint("name", "tenant_id", name="unique_tenant_dimension_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {
            "models",
            "dimension",
            "tenant",
            "texts",
            "labels",
            "measures",
            "filter",
            "hierarchies",
            "attributes",
            "dimension_attributes",
            "data_storage_fields",
            "composite_fields",
            "attributes_table",
            "text_table",
            "values_table",
            "pv_dictionary",
            "prompt",
        },
        "check_modified_fields": {
            "auth_relevant",
            "texts_time_dependency",
            "texts_language_dependency",
            "case_sensitive",
            "data_access_method",
            "business_key_representation",
            "dimension_id",
            "is_virtual",
            "type",
            "precision",
        },
    }

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    information_category: Mapped[str] = mapped_column(String, nullable=False)
    auth_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    texts_time_dependency: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    texts_language_dependency: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    attributes_time_dependency: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    precision: Mapped[int] = mapped_column(Integer, nullable=True, default=None)
    attributes_table_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage.id", ondelete="RESTRICT"), nullable=True
    )
    text_table_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage.id", ondelete="RESTRICT"), nullable=True
    )
    values_table_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage.id", ondelete="RESTRICT"), nullable=True
    )
    pv_dictionary_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("pv_dictionary.id", ondelete="RESTRICT"), nullable=True
    )
    prompt_id: Mapped[Optional[int]] = mapped_column(ForeignKey("prompts.id", ondelete="RESTRICT"), nullable=True)
    case_sensitive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    data_access_method: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    business_key_representation: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    dimension_id: Mapped[Optional[int]] = mapped_column(ForeignKey("dimension.id", ondelete="RESTRICT"), nullable=True)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)
    is_virtual: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))

    # Relationships
    models: Mapped[list["Model"]] = relationship("Model", secondary=DimensionModelRelation.__table__, lazy="selectin")
    model_relations: Mapped[list["DimensionModelRelation"]] = relationship(
        "DimensionModelRelation", lazy="selectin", viewonly=True
    )
    dimension: Mapped[Optional["Dimension"]] = relationship(
        "Dimension", remote_side=[id], foreign_keys=[dimension_id], lazy="joined"
    )
    tenant: Mapped["Tenant"] = relationship(back_populates="dimensions", foreign_keys=[tenant_id])
    texts: Mapped[list["TextLink"]] = relationship(
        back_populates="dimension", lazy="selectin", cascade="all,delete-orphan"
    )
    labels: Mapped[list["DimensionLabel"]] = relationship(
        back_populates="dimension", lazy="selectin", cascade="all,delete-orphan"
    )
    measures: Mapped[list["Measure"]] = relationship(back_populates="dimension", passive_deletes=True)
    filter: Mapped[list["DimensionFilter"]] = relationship(back_populates="dimension")
    hierarchies: Mapped[list["HierarchyMeta"]] = relationship(
        back_populates="base_dimensions",
        secondary=HierarchyBaseDimension.__table__,
        lazy="selectin",
    )
    attributes: Mapped[list["DimensionAttribute"]] = relationship(
        "DimensionAttribute",
        back_populates="dimension",
        cascade="all,delete-orphan",
        primaryjoin="Dimension.id == DimensionAttribute.dimension_id",
        lazy="selectin",
    )
    dimension_attributes: Mapped[list["DimensionAttribute"]] = relationship(
        "DimensionAttribute",
        back_populates="dimension_attribute",
        primaryjoin="Dimension.id == DimensionAttribute.dimension_attribute_id",
        viewonly=True,
        order_by="DimensionAttribute.id",
    )
    data_storage_fields: Mapped[list["DataStorageField"]] = relationship(
        back_populates="dimension",
        passive_deletes=True,
    )
    composite_fields: Mapped[list["CompositeField"]] = relationship(
        back_populates="dimension",
        passive_deletes=True,
    )
    attributes_table: Mapped[Optional["DataStorage"]] = relationship(
        "DataStorage",
        foreign_keys=[attributes_table_id],
        lazy="joined",
        uselist=False,
        cascade="all,delete-orphan",
        single_parent=True,
    )
    text_table: Mapped[Optional["DataStorage"]] = relationship(
        "DataStorage",
        foreign_keys=[text_table_id],
        lazy="joined",
        uselist=False,
        cascade="all,delete-orphan",
        single_parent=True,
    )
    values_table: Mapped[Optional["DataStorage"]] = relationship(
        "DataStorage",
        foreign_keys=[values_table_id],
        lazy="joined",
        uselist=False,
        cascade="all,delete-orphan",
        single_parent=True,
    )
    pv_dictionary: Mapped[Optional["PVDctionary"]] = relationship(
        "PVDctionary",
        foreign_keys=[pv_dictionary_id],
        lazy="joined",
        uselist=False,
        cascade="all,delete-orphan",
        single_parent=True,
    )
    prompt: Mapped[Optional["AIPrompt"]] = relationship(
        "AIPrompt",
        foreign_keys=[prompt_id],
        lazy="joined",
        uselist=False,
        cascade="all,delete-orphan",
        single_parent=True,
    )


class TextLink(Base, Versioned):
    """Вид текста для значений измерения"""

    __tablename__ = "text_link"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "dimension",
        }
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text_type: Mapped[str] = mapped_column(String, nullable=False)
    dimension_id: Mapped[int] = mapped_column(ForeignKey("dimension.id"), nullable=False)

    # Relationships
    dimension: Mapped["Dimension"] = relationship(back_populates="texts", foreign_keys=[dimension_id])


class DimensionLabel(Base, LabelMixin, Versioned):
    """Текстовое описание Composite."""

    __tablename__ = "dimension_label"  # type: ignore

    # Fields
    dimension_id: Mapped[int] = mapped_column(ForeignKey("dimension.id"), nullable=False)

    # Relationships
    dimension: Mapped["Dimension"] = relationship(back_populates="labels", foreign_keys=[dimension_id])


class DimensionAttribute(Base, Versioned):
    """Атрибут признака."""

    __tablename__ = "dimension_attribute"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "dimension",
            "dimension_attribute",
            "measure_attribute",
            "any_field_attribute",
        }
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    attribute_type: Mapped[str] = mapped_column(String, nullable=False, name="type")
    time_dependency: Mapped[bool] = mapped_column(Boolean, nullable=False)
    semantic_type: Mapped[str] = mapped_column(String, nullable=False)
    dimension_id: Mapped[int] = mapped_column(ForeignKey("dimension.id"), nullable=False)
    dimension_attribute_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("dimension.id", ondelete="RESTRICT"), nullable=True
    )
    measure_attribute_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("measure.id", ondelete="RESTRICT"), nullable=True
    )
    any_field_attribute_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("any_field.id", ondelete="RESTRICT"), nullable=True
    )

    # Relationships
    dimension: Mapped["Dimension"] = relationship(back_populates="attributes", foreign_keys=[dimension_id])
    labels: Mapped[list["DimensionAttributeLabel"]] = relationship(
        back_populates="dimension_attribute", lazy="selectin", cascade="all,delete"
    )
    dimension_attribute: Mapped[Optional["Dimension"]] = relationship(
        back_populates="dimension_attributes",
        foreign_keys=[dimension_attribute_id],
        lazy="joined",
    )
    measure_attribute: Mapped[Optional["Measure"]] = relationship(
        back_populates="dimension_attributes",
        foreign_keys=[measure_attribute_id],
        lazy="joined",
    )
    any_field_attribute: Mapped[Optional["AnyField"]] = relationship(
        back_populates="dimension_attributes",
        foreign_keys=[any_field_attribute_id],
        lazy="joined",
        cascade="all,delete",
    )


class DimensionAttributeLabel(Base, LabelMixin, Versioned):
    """Текстовое описание DimensionAttribute."""

    __tablename__ = "dimension_attribute_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "dimension_attribute",
        }
    }
    # Fields
    dimension_attribute_id: Mapped[str] = mapped_column(ForeignKey("dimension_attribute.id"), nullable=False)

    # Relationships
    dimension_attribute: Mapped["DimensionAttribute"] = relationship(
        back_populates="labels", foreign_keys=[dimension_attribute_id]
    )


class PVDctionary(Base, Versioned):
    """Свойства PV Dictionaries для измерения"""

    __tablename__ = "pv_dictionary"  # type: ignore

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    object_id: Mapped[int] = mapped_column(Integer, nullable=False)
    object_name: Mapped[str] = mapped_column(String, nullable=False)
    object_type: Mapped[str] = mapped_column(String, nullable=False)
    domain_name: Mapped[str] = mapped_column(String, nullable=False)
    domain_label: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, server_default=text("'PENDING'"), default="PENDING", nullable=False)
    msg: Mapped[str] = mapped_column(String, nullable=True)


class GroupDescription(Base, Versioned):
    __tablename__ = "group_description"  # type: ignore

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    entity_name: Mapped[str] = mapped_column(String(255))
    description: Mapped[Optional[str]] = mapped_column(String(8192), nullable=True)
    synonyms: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    few_shots: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class AIPrompt(Base, Versioned):
    """Промпты с описанием справочников для LLM."""

    __tablename__ = "prompts"  # type: ignore

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    analytic_role: Mapped[str] = mapped_column(String, nullable=False)
    purpose: Mapped[str] = mapped_column(String, nullable=False)
    key_features: Mapped[str] = mapped_column(String, nullable=False)
    data_type: Mapped[str] = mapped_column(String, nullable=False)
    subject_area: Mapped[str] = mapped_column(String, nullable=False)
    example_questions: Mapped[str] = mapped_column(Text, nullable=False)
    synonyms: Mapped[str] = mapped_column(Text, nullable=False)  # todo: узнать надо ли удалять
    markers: Mapped[str] = mapped_column(Text, nullable=False)
    notes: Mapped[str] = mapped_column(Text, nullable=False)

    ai_usage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    entity_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    domain_id: Mapped[int] = mapped_column(Integer, nullable=True, default=None)
    group_id: Mapped[int] = mapped_column(ForeignKey("group_description.id"), nullable=True)
    vector_search: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fallback_to_llm_values: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    preferable_columns: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    description: Mapped[str] = mapped_column(String(8192), nullable=True, default=None)
    few_shots: Mapped[str] = mapped_column(Text, nullable=True, default=None)

    group: Mapped[GroupDescription | None] = relationship(
        "GroupDescription",
        foreign_keys=[group_id],
        lazy="joined",
        single_parent=True,
    )
