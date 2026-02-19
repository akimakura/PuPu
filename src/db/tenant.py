from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped

from src.db.meta import Base
from src.db.mixins import LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.composite import Composite
    from src.db.data_storage import DataStorage
    from src.db.database import Database
    from src.db.dimension import Dimension
    from src.db.measure import Measure
    from src.db.model import Model


class Tenant(Base, Versioned):
    """
    Тенант - совокупность всех объектов системы для изоляции их друг от друга.
    В сущности необходимо для обеспечения уникальности имен объектов только в рамках одного тенанта.
    """

    __tablename__ = "tenant"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "data_storages",
            "dimensions",
            "measures",
            "composites",
            "databases",
            "models",
            "labels",
        },
    }

    # Fields
    name: Mapped[str] = mapped_column(String, primary_key=True)

    # Relationships
    labels: Mapped[list["TenantLabel"]] = relationship(
        back_populates="tenant",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    data_storages: Mapped[list["DataStorage"]] = relationship(back_populates="tenant")
    dimensions: Mapped[list["Dimension"]] = relationship(back_populates="tenant")
    measures: Mapped[list["Measure"]] = relationship(back_populates="tenant")
    composites: Mapped[list["Composite"]] = relationship(back_populates="tenant")
    databases: Mapped[list["Database"]] = relationship(back_populates="tenant")
    models: Mapped[list["Model"]] = relationship(back_populates="tenant", lazy="selectin")


class TenantLabel(Base, LabelMixin, Versioned):
    """Текстовое описание Model."""

    __tablename__ = "tenant_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {
            "tenant",
        },
    }

    # Fields
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)

    # Relationships
    tenant: Mapped["Tenant"] = relationship(back_populates="labels", foreign_keys=[tenant_id])
