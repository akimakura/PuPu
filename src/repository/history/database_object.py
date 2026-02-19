import datetime
from typing import Any, Callable, Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.database_object import DatabaseObject
from src.repository.history.base_history import BaseHistoryRepository

logger = EPMPYLogger(__name__)


class DatabaseObjectHistoryRepository(BaseHistoryRepository):
    """Репозиторий для версионирования database_object и сохранения истории."""

    def is_modified(self, database_object: DatabaseObject, new_definition: Optional[dict] = None) -> bool:
        """Проверяет, изменилось ли JSON-описание представления."""
        if new_definition is None:
            return False
        return database_object.json_definition != new_definition

    async def update_version(
        self,
        database_object: DatabaseObject,
        create: bool = False,
        forced_version: Optional[int] = None,
        forced_timestamp: Optional[datetime.datetime] = None,
        tenant_id: str | None = None,
    ) -> None:
        """Устанавливает версию, пользователя и timestamp для объекта database_object."""
        await self.try_set_version_to_created_obj(
            database_object,
            create,
            database_object.tenant_id,
            database_object.type,
            database_object.schema_name,
            database_object.name,
            forced_version=forced_version,
            forced_timestamp=forced_timestamp,
        )

    async def get_last_version(self, tenant_id: str, object_type: str, schema_name: str, name: str) -> int:
        """Возвращает последнюю версию database_object из history-таблицы."""
        database_object_history = DatabaseObject.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(database_object_history.version)).where(
                database_object_history.tenant_id == tenant_id,
                database_object_history.type == object_type,
                database_object_history.schema_name == schema_name,
                database_object_history.name == name,
            )
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def save_history(
        self,
        database_object: DatabaseObject,
        new_definition: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
        forced_version: Optional[int] = None,
    ) -> None:
        """Сохраняет историю database_object и увеличивает версию при изменениях."""
        if not deleted and not forced and not self.is_modified(database_object, new_definition):
            return None
        logger.info("DatabaseObject %s modified or deleted. Saving history.", database_object.name)
        copy_to_history: Callable[[Any, bool], None] = self.copy_obj_to_history  # type: ignore
        copy_to_history(database_object, deleted)
        new_version = database_object.version + 1 if forced_version is None else forced_version
        self.update_obj_version(database_object, datetime.datetime.now(datetime.UTC), None, new_version)
        await self.session.flush()
        logger.info("DatabaseObject %s saved with version %s", database_object.name, new_version - 1)
        return None
