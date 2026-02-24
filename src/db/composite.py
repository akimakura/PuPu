from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import FieldMixin, LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.any_field import AnyField
    from src.db.data_storage import DataStorage, DataStorageField
    from src.db.database_object import DatabaseObject
    from src.db.dimension import Dimension
    from src.db.measure import Measure
    from src.db.model import Model
    from src.db.tenant import Tenant


class CompositeModelRelation(Versioned, Base):
    """Связь между моделью и Composite."""

    __tablename__ = "composite_model_relation"  # type: ignore
    __table_args__ = (UniqueConstraint("composite_id", "model_id", name="unique_model_id_composite_id_combination"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    composite_id: Mapped[int] = mapped_column(ForeignKey("composite.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING", server_default=text("'PENDING'"))
    msg: Mapped[str] = mapped_column(String, nullable=True)
    is_owner: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))


class Composite(Versioned, Base):
    """Композит."""

    __tablename__ = "composite"  # type: ignore
    __table_args__ = (UniqueConstraint("name", "tenant_id", name="unique_tenant_composite_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {
            "database_objects",
            "datasources",
            "link_fields",
            "fields",
            "labels",
            "models",
            "tenant",
        },
        "check_modified_fields": {
            "link_type",
            "is_tech",
        },
    }

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)
    link_type: Mapped[str] = mapped_column(String, nullable=False)
    is_tech: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Relationships
    database_objects: Mapped[list["DatabaseObject"]] = relationship(
        back_populates="composite", cascade="all, delete-orphan"
    )
    models: Mapped[list["Model"]] = relationship("Model", secondary=CompositeModelRelation.__table__, lazy="selectin")
    model_relations: Mapped[list["CompositeModelRelation"]] = relationship(
        "CompositeModelRelation", lazy="selectin", viewonly=True
    )
    tenant: Mapped["Tenant"] = relationship(back_populates="composites", foreign_keys=[tenant_id])
    labels: Mapped[list["CompositeLabel"]] = relationship(
        back_populates="composite", lazy="selectin", cascade="all,delete-orphan"
    )
    fields: Mapped[list["CompositeField"]] = relationship(
        back_populates="composite", lazy="selectin", cascade="all,delete-orphan", order_by="CompositeField.id"
    )
    datasources: Mapped[list["CompositeDatasource"]] = relationship(
        "CompositeDatasource",
        back_populates="composite",
        lazy="selectin",
        cascade="all,delete-orphan",
        primaryjoin="Composite.id == CompositeDatasource.composite_id",
        order_by="CompositeDatasource.id",
    )
    link_fields: Mapped[list["CompositeLinkFields"]] = relationship(
        back_populates="composite", lazy="selectin", cascade="all,delete-orphan", order_by="CompositeLinkFields.id"
    )


class CompositeDatasource(Versioned, Base):
    """Источники данных, которые привязаны к композиту."""

    __tablename__ = "composite_datasource"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "composite_datasource",
            "datastorage_datasource",
            "composite",
        }
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    undescribed_ref_object_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    undescribed_ref_object_schema_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    composite_datasource_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("composite.id", ondelete="RESTRICT"), nullable=True
    )
    datastorage_datasource_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage.id", ondelete="RESTRICT"), nullable=True
    )
    composite_id: Mapped[int] = mapped_column(ForeignKey("composite.id"), nullable=False)

    # Relationships
    composite_datasource: Mapped[Optional["Composite"]] = relationship(
        backref="datasource_attributes", foreign_keys=[composite_datasource_id], lazy="joined"
    )
    datastorage_datasource: Mapped[Optional["DataStorage"]] = relationship(
        backref="datasource_attributes", foreign_keys=[datastorage_datasource_id], lazy="joined"
    )
    composite: Mapped["Composite"] = relationship(back_populates="datasources", foreign_keys=[composite_id])


class CompositeLinkFields(Versioned, Base):
    """Порядок полей для join Composite."""

    __tablename__ = "composite_link_fields"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "left_composite_field",
            "left_data_storage_field",
            "right_composite_field",
            "right_data_storage_field",
            "composite",
        }
    }

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    composite_id: Mapped[int] = mapped_column(ForeignKey("composite.id"), nullable=False)
    left_type: Mapped[str] = mapped_column(String, nullable=False)
    right_type: Mapped[str] = mapped_column(String, nullable=False)
    left_undescribed_ref_object_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    right_undescribed_ref_object_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    left_undescribed_ref_object_field_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    right_undescribed_ref_object_field_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    left_composite_field_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("composite_field.id", ondelete="RESTRICT"), nullable=True
    )
    left_data_storage_field_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage_field.id", ondelete="RESTRICT"), nullable=True
    )
    right_composite_field_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("composite_field.id", ondelete="RESTRICT"), nullable=True
    )
    right_data_storage_field_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage_field.id", ondelete="RESTRICT"), nullable=True
    )

    # Relationships
    left_composite_field: Mapped[Optional["CompositeField"]] = relationship(
        "CompositeField", foreign_keys=[left_composite_field_id], lazy="joined"
    )
    left_data_storage_field: Mapped[Optional["DataStorageField"]] = relationship(
        "DataStorageField", foreign_keys=[left_data_storage_field_id], lazy="joined"
    )
    right_composite_field: Mapped[Optional["CompositeField"]] = relationship(
        "CompositeField", foreign_keys=[right_composite_field_id], lazy="joined"
    )
    right_data_storage_field: Mapped[Optional["DataStorageField"]] = relationship(
        "DataStorageField", foreign_keys=[right_data_storage_field_id], lazy="joined"
    )
    composite: Mapped["Composite"] = relationship(back_populates="link_fields", foreign_keys=[composite_id])


class CompositeLabel(Base, LabelMixin, Versioned):
    """Текстовое описание Composite."""

    __tablename__ = "composite_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "composite",
        }
    }
    # Fields
    composite_id: Mapped[int] = mapped_column(ForeignKey("composite.id"), nullable=False)

    # Relationships
    composite: Mapped["Composite"] = relationship(back_populates="labels", foreign_keys=[composite_id])


class CompositeField(Base, FieldMixin, Versioned):
    """Поля Composite."""

    __tablename__ = "composite_field"  # type: ignore
    __table_args__ = (UniqueConstraint("composite_id", "name", name="unique_composite_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {
            "composite",
            "labels",
            "dimension",
            "measure",
            "any_field",
            "datasource_links",
        }
    }
    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    composite_id: Mapped[int] = mapped_column(ForeignKey("composite.id"), nullable=False)

    # Relationships
    composite: Mapped["Composite"] = relationship(back_populates="fields", foreign_keys=[composite_id], lazy="joined")
    labels: Mapped[list["CompositeFieldLabel"]] = relationship(
        back_populates="composite_field", lazy="selectin", cascade="all,delete-orphan"
    )
    dimension: Mapped[Optional["Dimension"]] = relationship(back_populates="composite_fields", lazy="selectin")
    measure: Mapped[Optional["Measure"]] = relationship(back_populates="composite_fields", lazy="selectin")
    any_field: Mapped[Optional["AnyField"]] = relationship(
        back_populates="composite_fields", lazy="selectin", cascade="all,delete"
    )
    datasource_links: Mapped[list["DatasourceLink"]] = relationship(
        back_populates="composite_field",
        lazy="selectin",
        foreign_keys="DatasourceLink.composite_field_id",
        cascade="all,delete-orphan",
    )


class DatasourceLink(Base, Versioned):
    """Источники данных, привязанные к полям композита."""

    __tablename__ = "datasource_link"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "composite_field",
            "composite_field_ref",
            "data_storage_field_ref",
        }
    }
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    composite_field_id: Mapped[int] = mapped_column(ForeignKey("composite_field.id"), nullable=False)
    undescribed_ref_object_field_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    undescribed_ref_object_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    datasource_type: Mapped[str] = mapped_column(String, nullable=False)
    composite_field_ref_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("composite_field.id", ondelete="RESTRICT"), nullable=True
    )
    data_storage_field_ref_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_storage_field.id", ondelete="RESTRICT"), nullable=True
    )
    composite_field: Mapped["CompositeField"] = relationship(
        "CompositeField",
        foreign_keys=[composite_field_id],
        back_populates="datasource_links",
    )
    composite_field_ref: Mapped[Optional["CompositeField"]] = relationship(
        "CompositeField",
        foreign_keys=[composite_field_ref_id],
        lazy="joined",
    )
    data_storage_field_ref: Mapped[Optional["DataStorageField"]] = relationship(
        "DataStorageField", foreign_keys=[data_storage_field_ref_id], lazy="joined"
    )


class CompositeFieldLabel(Base, LabelMixin, Versioned):
    """Текстовое описание Composite."""

    __tablename__ = "composite_field_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "composite_field",
        }
    }
    # Fields
    composite_field_id: Mapped[int] = mapped_column(ForeignKey("composite_field.id"), nullable=False)

    # Relationships
    composite_field: Mapped["CompositeField"] = relationship(back_populates="labels", foreign_keys=[composite_field_id])
