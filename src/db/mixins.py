"""Миксины для моделей SQLAlchemy"""

from typing import Optional

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm.base import Mapped


class LabelMixin:
    """Миксин для моделей текстовых описаний."""

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    language: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    text: Mapped[str] = mapped_column(String, nullable=False)


class FieldTypeMixin:
    """Миксин для типов полей DataStorage и Composite."""

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    precision: Mapped[int] = mapped_column(Integer, nullable=False)


class FieldMixin:
    """Mixin для обобщения CompositeField и DataStorageField."""

    # Fields
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    semantic_type: Mapped[str] = mapped_column(String, nullable=False)
    sql_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    field_type: Mapped[str] = mapped_column(String, nullable=False, name="type")
    dimension_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "dimension.id",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    measure_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "measure.id",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    any_field_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "any_field.id",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
