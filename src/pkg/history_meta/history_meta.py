"""
Модуль history_meta.py реализует миксин-класс для версионирования ORM-моделей SQLAlchemy, а также вспомогательные функции.

Основные возможности:
- Автоматическое создание исторических таблиц для моделей, наследующих Versioned.

Ключевые компоненты:
- Versioned: Миксин-класс, который добавляет версионирование к модели.
- _history_mapper: Внутренняя функция, настраивающая отображение исторических таблиц и классов.
- Вспомогательные функции для работы с колонками, таблицами и свойствами ORM.

Использование:
1. Наследуйте вашу модель от Versioned.
"""

import datetime
from typing import TYPE_CHECKING, Any, Optional, Sequence, Type

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
    and_,
    event,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import Mapper, mapped_column
from sqlalchemy.orm.base import Mapped
from sqlalchemy.sql.schema import Column as SAColumn
from sqlalchemy.util import OrderedDict

logger = EPMPYLogger(__name__)


def is_col_references_table(col: Column, table: Table) -> bool:
    """
    Проверяет, содержит ли столбец col внешний ключ на таблицу table.

    Args:
        col (Column): Столбец SQLAlchemy Column.
        table (Table): Таблица SQLAlchemy Table.
    Returns:
        bool:  содержит ли столбец col внешний ключ на таблицу table
    """
    return any(fk.references(table) for fk in col.foreign_keys)


def _create_history_table(
    local_mapper: Mapper,
) -> tuple[Any, dict]:
    """
    Создаёт таблицу истории для указанного маппера и возвращает её вместе с метаинформацией.

    Args:
        local_mapper (Mapper): Маппер основной модели.

    Returns:
        Tuple[Any, dict]: Кортеж из созданной таблицы истории и словаря с метаинформацией.
    """
    version_meta = {"version_meta": True}
    history_table = local_mapper.local_table.to_metadata(  # type: ignore
        local_mapper.local_table.metadata,  # type: ignore
        name=local_mapper.local_table.name + "_history",  # type: ignore
    )
    for idx in history_table.indexes:
        if idx.name is not None:
            idx.name += "_history"
        idx.unique = False
    return history_table, version_meta


def _add_super_fks(history_table: Any, super_fks: list) -> None:
    """
    Добавляет внешние ключи к родительским таблицам в таблицу истории.

    Args:
        history_table (Any): Таблица истории.
        super_fks (list): Список внешних ключей.
    """
    if super_fks:
        history_table.append_constraint(ForeignKeyConstraint(*zip(*super_fks)))  # type: ignore


def _add_columns_single_table_inheritance(local_mapper: Mapper, super_mapper: Mapper) -> None:
    """
    Добавляет новые колонки в таблицу истории для single table inheritance.

    Args:
        local_mapper (Mapper): Маппер дочерней модели.
        super_mapper (Mapper): Маппер родительской модели.
    """
    super_history_table = super_mapper.local_table.metadata.tables[super_mapper.local_table.name + "_history"]  # type: ignore
    for column in local_mapper.local_table.c:
        if column.key not in super_history_table.c:
            col = Column(column.name, column.type, nullable=column.nullable)
            super_history_table.append_column(col)


def _add_version_and_deleted_columns_to_history_table(history_table: Table, version_meta: dict) -> None:
    """
    Добавляет служебные колонки version и deleted в таблицу истории.

    Args:
        history_table (Any): Таблица истории.
        version_meta (dict): Метаинформация для служебных колонок.
    """
    history_table.append_column(
        Column(
            name="version",
            type_=Integer,
            primary_key=True,
            autoincrement=False,
            info=version_meta,
        ),
        replace_existing=True,
    )
    history_table.append_column(
        Column(
            name="deleted",
            type_=Boolean,
            default=False,
            info=version_meta,
        ),
        replace_existing=True,
    )


def _set_active_history(local_mapper: Mapper) -> None:
    """
    Устанавливает флаг active_history для всех свойств маппера.

    Args:
        local_mapper (Mapper): Маппер модели.
    """
    for prop in local_mapper.iterate_properties:
        prop.active_history = True


def _add_version_column_to_history_table(local_mapper: Mapper, history_table: Table, cls: Type) -> None:
    """
    Добавляет колонку version в основную таблицу и настраивает её для отслеживания версий.

    Args:
        local_mapper (Mapper): Маппер основной модели.
        history_table (Table): Таблица истории.
        cls (Type): Класс основной модели.
    """
    local_mapper.local_table.append_column(  # type: ignore
        Column(
            "version",
            Integer,
            default=lambda context: _default_version_from_history(context, history_table, local_mapper.local_table),
            nullable=False,
        ),
        replace_existing=True,
    )
    local_mapper.add_property("version", local_mapper.local_table.c.version)  # type: ignore
    if cls.use_mapper_versioning:
        local_mapper.version_id_col = local_mapper.local_table.c.version


def _copy_columns_and_properties(
    local_mapper: Mapper,
    super_mapper: Optional[Mapper],
    super_history_mapper: Optional[Mapper],
    history_table: Any,
    properties: OrderedDict,
    super_fks: list,
) -> Optional[SAColumn]:
    """
    Копирует колонки и свойства из основной таблицы в таблицу истории, настраивает связи и возвращает колонку для полиморфизма (если есть).

    Args:
        local_mapper (Mapper): Маппер основной модели.
        super_mapper (Optional[Mapper]): Маппер родительской модели.
        super_history_mapper (Optional[Mapper]): Маппер истории родительской модели.
        history_table (Any): Таблица истории.
        properties (OrderedDict): Свойства для передачи в маппер истории.
        super_fks (list): Список внешних ключей для родительских таблиц.

    Returns:
        Optional[SAColumn]: Колонка для полиморфизма, если она есть, иначе None.
    """
    polymorphic_on = None
    for orig_c, history_c in zip(local_mapper.local_table.c, history_table.c):
        orig_c.info["history_copy"] = history_c
        history_c.unique = False
        history_c.default = history_c.server_default = None
        history_c.autoincrement = False
        if super_mapper and is_col_references_table(orig_c, super_mapper.local_table):  # type: ignore
            assert super_history_mapper is not None
            super_fks.append(
                (
                    history_c.key,
                    list(super_history_mapper.local_table.primary_key)[0],
                )
            )
        if orig_c is local_mapper.polymorphic_on:
            polymorphic_on = history_c

        orig_prop = local_mapper.get_property_by_column(orig_c)
        if len(orig_prop.columns) > 1 or orig_prop.columns[0].key != orig_prop.key:
            properties[orig_prop.key] = tuple(col.info["history_copy"] for col in orig_prop.columns)
    return polymorphic_on


def _default_version_from_history(context: Any, history_table: Table, local_table: Any) -> int:
    """
    Возвращает значение по умолчанию для колонки version как максимум среди уже существующих версий +1.

    Args:
        context (Any): Контекст выполнения SQLAlchemy.
        history_table (Table): Историческая таблица SQLAlchemy.
        local_table (Any): Таблица SQLAlchemy.

    Returns:
        Any: Следующее значение версии для строки.
    """
    current_parameters = context.get_current_parameters()
    unique_columns = []
    for constraint in list(history_table.constraints):
        if isinstance(constraint, UniqueConstraint):
            for col in constraint.columns:
                unique_columns.append(col.name)
    if not unique_columns:
        return context.connection.scalar(
            select(func.coalesce(func.max(history_table.c.version), 0) + 1).where(
                and_(
                    *[
                        history_table.c[c.name] == current_parameters.get(c.name)  # type: ignore
                        for c in inspect(local_table).primary_key  # type: ignore
                    ]
                )
            )
        )
    else:
        return context.connection.scalar(
            select(func.coalesce(func.max(history_table.c.version), 0) + 1).where(
                and_(
                    *[
                        history_table.c[column] == current_parameters.get(column)  # type: ignore
                        for column in unique_columns
                        if column != "version"  # type: ignore
                    ]
                )
            )
        )


def _cleanup_constraints(history_table: Table) -> None:
    """
    Удаляет все ограничения, кроме первичных ключей и внешних ключей, из таблицы истории.

    Args:
        history_table (Any): Таблица истории.
    """
    # Unique constraints
    unique_columns = set()
    for constraint in list(history_table.constraints):
        if isinstance(constraint, UniqueConstraint):
            for col in constraint.columns:
                unique_columns.add(col.name)
    if len(unique_columns) >= 1:
        unique_columns.add("version")
    unique_columns_list = list(unique_columns)
    unique_columns_list.sort()
    for const in list(history_table.constraints):
        if not isinstance(const, (PrimaryKeyConstraint,)):
            history_table.constraints.discard(const)
    if unique_columns_list:
        history_table.append_constraint(UniqueConstraint(*unique_columns_list))


def _create_versioned_class(
    cls: Type,
    bases: tuple,
    history_table: Optional[Any],
    super_history_mapper: Optional[Mapper],
    local_mapper: Mapper,
    polymorphic_on: Optional[SAColumn],
    properties: OrderedDict,
) -> type:
    """
    Создаёт класс истории для модели с заданными параметрами.

    Args:
        cls (Type): Класс основной модели.
        bases (tuple): Базовые классы для класса истории.
        history_table (Optional[Any]): Таблица истории.
        super_history_mapper (Optional[Mapper]): Маппер истории родительской модели.
        local_mapper (Mapper): Маппер основной модели.
        polymorphic_on (Optional[SAColumn]): Колонка для полиморфизма.
        properties (OrderedDict): Свойства для передачи в маппер истории.

    Returns:
        type: Класс истории.
    """
    versioned_cls = type(
        "%sHistory" % cls.__name__,
        bases,
        {
            "_history_mapper_configured": True,
            "__table__": history_table,
            "__mapper_args__": {
                "inherits": super_history_mapper,
                "polymorphic_identity": local_mapper.polymorphic_identity,
                "polymorphic_on": polymorphic_on,
                "properties": properties,
            },
        },
    )
    return versioned_cls


class Versioned:
    """
    Базовый класс для моделей, поддерживающих версионирование записей.
    """

    use_mapper_versioning = False
    if TYPE_CHECKING:
        version: Mapped[int]
    # Если True, также назначает колонку version для отслеживания маппером

    user: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    timestamp: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.datetime.now(datetime.UTC),
        server_default=text("CURRENT_TIMESTAMP"),
    )
    __versioned__: dict[str, set] = {
        "not_versioned_fields": set(),
        "check_modified_fields": set(),
    }
    __table_args__: Sequence[Any] = ({"sqlite_autoincrement": True},)
    # Использовать sqlite_autoincrement, чтобы гарантировать уникальные целочисленные значения
    # для новых строк, даже если некоторые строки были удалены.

    def __init_subclass__(cls) -> None:
        """
        Переопределяет метод __init_subclass__ для инициализации history_mapper.
        Регистрирует обработчик событий после построения маппера, если инспектор недоступен.
        """
        insp = inspect(cls, raiseerr=False)

        if insp is not None:
            cls._history_mapper(insp)
        else:

            @event.listens_for(cls, "after_mapper_constructed")
            def _mapper_constructed(mapper: Mapper, class_: Any) -> None:
                cls._history_mapper(mapper)

        super().__init_subclass__()

    @classmethod
    def _history_mapper(cls, local_mapper: Mapper) -> None:
        """
        Конфигурирует отображение исторической таблицы и класса для указанного маппера.
        Создаёт *_history таблицу, копирует структуру, добавляет служебные поля (version, changed), настраивает связи и наследование.

        Args:
            local_mapper (Mapper): SQLAlchemy Mapper для основной модели.
        """
        obj_cls = local_mapper.class_

        if obj_cls.__dict__.get("_history_mapper_configured", False):
            return

        obj_cls._history_mapper_configured = True

        super_mapper = local_mapper.inherits
        polymorphic_on = None
        super_fks: list = []
        properties: OrderedDict = OrderedDict()

        if super_mapper:
            super_history_mapper = super_mapper.class_.__history_mapper__
        else:
            super_history_mapper = None

        if not super_mapper or local_mapper.local_table is not super_mapper.local_table:
            history_table, version_meta = _create_history_table(local_mapper)
            polymorphic_on = _copy_columns_and_properties(
                local_mapper, super_mapper, super_history_mapper, history_table, properties, super_fks
            )

            _add_version_and_deleted_columns_to_history_table(history_table, version_meta)
            _cleanup_constraints(history_table)
            if super_mapper:
                super_fks.append(("version", super_history_mapper.local_table.c.version))

            _add_super_fks(history_table, super_fks)

        else:
            history_table = None
            _add_columns_single_table_inheritance(local_mapper, super_mapper)

        if not super_mapper:
            _add_version_column_to_history_table(local_mapper, history_table, obj_cls)

        # устанавливаем флаг "active_history" на
        # все атрибуты, связанные с колонками, чтобы всегда загружалась
        # старая версия информации (сейчас ставится на все атрибуты)
        _set_active_history(local_mapper)
        super_mapper = local_mapper.inherits

        if super_history_mapper:
            bases = (super_history_mapper.class_,)

        else:
            bases = local_mapper.base_mapper.class_.__bases__  # type: ignore

        versioned_cls = _create_versioned_class(
            obj_cls, bases, history_table, super_history_mapper, local_mapper, polymorphic_on, properties
        )

        obj_cls.__history_mapper__ = versioned_cls.__mapper__  # type: ignore


def get_table_attrs(obj: Any) -> dict[str, Any]:
    """
    Возвращает словарь атрибутов SQLAlchemy-таблицы объекта.

    Args:
        obj (Any): Объект SQLAlchemy модели

    Returns:
        dict[str, Any]: Словарь в формате {имя_атрибута: значение},
        содержащий все колонки таблицы, связанные с моделью
    """
    return {column.key: getattr(obj, column.key) for column in inspect(obj).mapper.column_attrs}


def obj_attr_changed(
    obj: Any, attribute: str, new_value: Any, ref_attribute: Optional[str] = None, allow_new_value_none: bool = False
) -> bool:
    """
    Проверяет, изменилось ли значение атрибута объекта по сравнению с новым значением.

    Args:
        obj (Any): Объект для проверки
        attribute (str): Имя атрибута объекта
        new_value (Any): Новое значение для сравнения
        ref_attribute (Optional[str]): Необязательный параметр - имя атрибута вложенного объекта для проверки
        allow_new_value_none (bool): Если True, разрешает сравнение с None

    Returns:
        bool: True, если текущее значение атрибута отличается от new_value, иначе False

    Примечание:
        При использовании ref_attr проверяет вложенный атрибут через getattr(obj, attr).ref_attr
        Если allow_new_value_none=False и new_value=None, всегда возвращает False
    """
    if not allow_new_value_none and new_value is None:
        return False
    if ref_attribute:
        ref_obj = getattr(obj, attribute, None)
        if ref_obj:
            attr_value = getattr(ref_obj, ref_attribute, None)
        else:
            attr_value = None
    else:
        attr_value = getattr(obj, attribute, None)
    return attr_value != new_value
