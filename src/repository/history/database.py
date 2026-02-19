"""
Модуль реализует репозиторий для работы с историей баз данных (Database).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.database import Database
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.history.base_history import BaseHistoryRepository

logger = EPMPYLogger(__name__)


class DatabaseHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей баз данных (Database).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для Database-объектов.

    Обрабатывает:
    - Проверку изменений в структуре Database
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def is_modified(self, database: Database, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в Database-объект.

        Args:
            database (Database): Объект Database для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        edit_labels = edit_model.get("labels") is not None
        edit_connections = edit_model.get("connections") is not None
        edited_fields = False
        for field_name in list(database.__class__.__versioned__["check_modified_fields"]):
            edited_fields |= obj_attr_changed(database, field_name, edit_model.get(field_name))
        return edit_labels or edit_connections or edited_fields

    async def update_version(
        self,
        database: Database,
        create: bool = False,
        forced_version: int | None = None,
        forced_timestamp: datetime.datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию Database-объекта и всех связанных с ним объектов.

        Args:
            database (Database): Объект Database для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id (str): Идентификатор тенанта (опционально)
        """
        await self.try_set_version_to_created_obj(database, create, database.tenant_id, database.name)
        self.update_obj_version(database.labels, database.timestamp, database.user, database.version)  # type: ignore
        self.update_obj_version(database.connections, database.timestamp, database.user, database.version)  # type: ignore
        for connection in database.connections:
            self.update_obj_version(connection.ports, database.timestamp, database.user, database.version)  # type: ignore

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Получает последнюю версию Database-объекта.

        Args:
            tenant_id (str): Идентификатор тенанта
            name (str): Имя Database-объекта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        DatabaseHistory = Database.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(DatabaseHistory.version)).where(  # type: ignore
                DatabaseHistory.tenant_id == tenant_id, DatabaseHistory.name == name
            )
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def save_history(
        self,
        database: Database,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
    ) -> None:
        """
        Сохраняет историческую запись для Database-объекта.

        Args:
            database (Database): Объект Database для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(database, edit_model):
            return None
        logger.info("Database %s modified or deleted. Saving history.", database.name)
        await self.copy_obj_to_history(database.labels, deleted)
        await self.copy_obj_to_history(database.connections, deleted)
        for connection in database.connections:
            await self.copy_obj_to_history(connection.ports, deleted)
        await self.copy_obj_to_history(database, deleted)
        self.update_obj_version(database, datetime.datetime.now(datetime.UTC), None, database.version + 1)  # type: ignore
        await self.session.flush()
        logger.info("Database %s saved with version %s", database.name, database.version - 1)  # type: ignore
        return None
