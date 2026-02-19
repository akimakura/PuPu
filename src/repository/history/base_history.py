"""
Модуль `base_history` предоставляет базовую реализацию для работы с историей изменений объектов в системе версионного контроля.

Класс `BaseHistoryRepository` является абстрактным родительским классом, который определяет интерфейс и базовые методы для управления версиями объектов, их хранением в истории и обновлением метаданных (timestamp, пользователь, версия). Основные функции:

1. **Управление версиями**:
   - Автоматическое определение следующей версии объекта (`get_last_version`).
   - Принудительное задание версии и временной метки при создании/обновлении объекта.
   - Логирование создания новых версий объектов.

2. **История изменений**:
   - Копирование объектов в историческую таблицу (`_copy_obj_to_history`).
   - Поддержка как отдельных объектов, так и списков объектов.

3. **Обновление метаданных**:
   - Установка пользовательского имени, времени изменения и номера версии.
   - Обработка флага удаления (`deleted`) при сохранении истории.

Класс требует реализации абстрактных методов (`is_modified`, `update_version`, `get_last_version`, `save_history`) в дочерних классах для конкретной логики работы с моделью данных. Все операции выполняются с использованием асинхронной сессии SQLAlchemy и логгера `EPMPYLogger`.

"""

import datetime
from typing import Any, Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import UniqueConstraint, and_, inspect, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.pkg.history_meta.history_meta import get_table_attrs
from src.utils.validators import get_username_by_token

logger = EPMPYLogger(__name__)


class BaseHistoryRepository:

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.user = get_username_by_token()

    def is_modified(self, obj: Any, edit_model: Optional[dict] = None) -> bool:
        raise NotImplementedError

    async def update_version(
        self,
        obj: Any,
        create: bool = False,
        forced_version: int | None = None,
        forced_timestamp: datetime.datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        raise NotImplementedError

    async def get_last_version(self, *args: Any, **kwargs: Any) -> int:
        raise NotImplementedError

    async def save_history(self, obj: Any, edit_obj: dict, deleted: bool = False, forced: bool = False) -> None:
        raise NotImplementedError

    @staticmethod
    def get_new_version(obj: Any) -> int:
        return obj.version + 1

    async def try_set_version_to_created_obj(
        self,
        obj: Any,
        create: bool,
        *args: Any,
        forced_version: Optional[int] = None,
        forced_timestamp: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Настраивает версию, временную метку и пользователя для объекта при его создании.

        Если параметр `create` истинен, функция:
        1. Устанавливает `obj.timestamp` в текущее время UTC (или переданное через `forced_timestamp`)
        2. Присваивает `obj.user` значение из `self.user`
        3. Определяет `obj.version` как последнюю версию + 1 (или использует `forced_version`)
        4. Логирует информацию, если объект имеет версию > 1 (при отсутствии принудительного значения)

        Args:
            obj (Any): Объект для настройки атрибутов
            create (bool): Флаг, определяющий необходимость установки атрибутов
            *args: Аргументы для метода `get_last_version`
            forced_version (Optional[int]): Принудительное значение версии
            forced_timestamp (Any): Принудительная временная метка (должна быть datetime)
            **kwargs: Именованные аргументы для метода `get_last_version`
        """
        if create:
            obj.timestamp = datetime.datetime.now(datetime.UTC) if forced_timestamp is None else forced_timestamp
            obj.user = self.user
            obj.version = await self.get_last_version(*args, **kwargs) + 1 if forced_version is None else forced_version
            if obj.version > 1 and forced_version is None:
                logger.info(
                    "There was already a version of this object in history. A object with version %s was created",
                    obj.version,
                )

    def update_obj_version(
        self,
        obj: Any,
        timestamp: Any = None,
        user: Optional[str] = None,
        version: Optional[int] = None,
    ) -> None:
        """
        Обновляет атрибуты version, user и timestamp у переданного объекта или списка объектов.

        Args:
            obj (Any): Объект или список объектов для обновления. Если None — ничего не делает.
            timestamp (Any): Новое значение временной метки (по умолчанию None).
            user (Optional[str]): Новый пользователь, который вносит изменения (по умолчанию берётся из self.user).
            version (Optional[int]): Новая версия объекта (по умолчанию None).
        """
        if user is None:
            user = self.user
        if isinstance(obj, list):
            for item in obj:
                item.version = version
                item.user = user
                item.timestamp = timestamp
        elif obj is not None:
            obj.version = version
            obj.user = user
            obj.timestamp = timestamp

    async def _copy_obj_to_history(
        self,
        obj: Any,
        deleted: bool = False,
    ) -> None:
        if obj is None:
            return

        history_table = obj.__class__.__history_mapper__.class_
        original_table = obj.__class__

        # Получаем поля для поиска: сначала unique constraints, потом primary key
        pk_fields, unique_fields = self._get_search_fields(history_table)
        # Ищем существующий объект в истории
        existing_hist = None
        if unique_fields:
            existing_hist = await self._find_history_by_fields(history_table, obj, unique_fields)
        if not existing_hist and pk_fields:
            existing_hist = await self._find_history_by_fields(history_table, obj, pk_fields)
        attrs = get_table_attrs(obj)
        if deleted:
            attrs["deleted"] = True

        if existing_hist:
            # Обновляем существующий
            for key, value in attrs.items():
                if key not in original_table.__versioned__.get("not_versioned_fields", set()):
                    setattr(existing_hist, key, value)
        else:
            # Создаем новый
            hist_obj = history_table()
            for key, value in attrs.items():
                if key not in original_table.__versioned__.get("not_versioned_fields", set()):
                    setattr(hist_obj, key, value)
            self.session.add(hist_obj)

    def _get_search_fields(self, model_class: Any) -> tuple[list[str], list[str]]:
        """Возвращает поля для поиска: unique constraints ИЛИ primary key."""
        # Инспектируем ТАБЛИЦУ, а не mapper
        mapper = inspect(model_class)
        table = mapper.local_table  # или mapper.persist_selectable

        pk_fields = set()
        unique_fields = set()

        # Ищем unique constraints из таблицы
        for constraint in table.constraints:
            if isinstance(constraint, UniqueConstraint):
                unique_fields.update([col.name for col in constraint.columns])
        # Берем primary key constraints, если нет unique constraints
        pk_columns = [col.name for col in table.primary_key.columns]
        pk_fields = set(pk_columns)
        return list(pk_fields), list(unique_fields)

    async def _find_history_by_fields(self, history_table: Any, obj: Any, search_fields: list[str]) -> Optional[Any]:
        """Ищет по определенным полям (unique или PK)."""

        filters = []
        attrs = get_table_attrs(obj)

        for field in search_fields:
            if field in attrs:
                filters.append(getattr(history_table, field) == attrs[field])

        if not filters:
            return None
        stmt = select(history_table).filter(and_(*filters))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def copy_obj_to_history(
        self,
        obj: Any,
        deleted: bool = False,
    ) -> None:
        """
        Обёртка для `_copy_obj_to_history`, поддерживающая как отдельные объекты, так и списки.

        Args:
            obj (Any): Объект или список объектов для сохранения в истории.
            deleted (bool): Флаг удаления, передаваемый внутрь `_copy_obj_to_history`.

        """
        if isinstance(obj, list):
            for item in obj:
                await self._copy_obj_to_history(item, deleted)
        elif obj is not None:
            await self._copy_obj_to_history(obj, deleted)
