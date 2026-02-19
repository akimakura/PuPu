from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, ForeignKey, Integer, String, UniqueConstraint, text
from sqlalchemy.orm import mapped_column, relationship
from sqlalchemy.orm.base import Mapped
from sqlalchemy.types import UUID

from src.db.meta import Base
from src.db.mixins import LabelMixin
from src.pkg.history_meta.history_meta import Versioned

if TYPE_CHECKING:
    from src.db.database import Database
    from src.db.tenant import Tenant


class Model(Base, Versioned):
    """
    Модель - совокупность: признаков, показателей, хранилищ, композитов
    других объектов принадлежащей одной области системы.
    """

    __tablename__ = "model"  # type: ignore
    __table_args__ = (UniqueConstraint("name", "tenant_id", name="unique_tenant_model_name_combination"),)
    __versioned__ = {
        "not_versioned_fields": {"database", "tenant", "labels"},
        "check_modified_fields": {
            "schema_name",
            "aor_space_id",
        },
    }

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    database_id: Mapped[int] = mapped_column(ForeignKey("database.id"), nullable=False)
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.name"), nullable=False)
    schema_name: Mapped[str] = mapped_column(String, nullable=False)
    dimension_tech_fields: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    aor_space_id: Mapped[Optional[UUID]] = mapped_column(UUID, nullable=True)
    # Relationships
    labels: Mapped[list["ModelLabel"]] = relationship(
        back_populates="model",
        lazy="selectin",
        cascade="all, delete-orphan",
    )
    database: Mapped["Database"] = relationship(
        back_populates="models",
        foreign_keys=[database_id],
        lazy="joined",
    )
    tenant: Mapped["Tenant"] = relationship(back_populates="models", foreign_keys=[tenant_id])

    def __repr__(self) -> str:
        return f"Model(id={self.id},name={self.name})"


class ModelLabel(Base, LabelMixin, Versioned):
    """Текстовое описание Model."""

    __tablename__ = "model_label"  # type: ignore
    __versioned__ = {
        "not_versioned_fields": {"model"},
    }

    # Fields
    model_id: Mapped[int] = mapped_column(ForeignKey("model.id"), nullable=False)

    # Relationships
    model: Mapped["Model"] = relationship(back_populates="labels", foreign_keys=[model_id])

    def __repr__(self) -> str:
        return f"ModelLabel(id={self.id},model_id={self.model_id})"
