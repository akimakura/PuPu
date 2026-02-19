"""
Модуль реализует репозиторий для работы с историей модели (Model).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Optional

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.model import Model
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.history.base_history import BaseHistoryRepository

logger = EPMPYLogger(__name__)


class ModelHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей изменений модели (Model).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для Model-объектов.

    Обрабатывает:
    - Проверку изменений в структуре Model
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def is_modified(self, model: Model, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в Model-объект.

        Args:
            model (Model): Объект Model для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        edit_labels = edit_model.get("labels") is not None
        edit_database = obj_attr_changed(model, "database", edit_model.get("database_id"), ref_attribute="name")
        edited_fields = False
        for field_name in list(model.__class__.__versioned__["check_modified_fields"]):
            edited_fields |= obj_attr_changed(model, field_name, edit_model.get(field_name))
        return edit_database or edit_labels or edited_fields

    async def update_version(
        self,
        model: Model,
        create: bool = False,
        forced_version: int | None = None,
        forced_timestamp: datetime.datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию Model-объекта и всех связанных с ним объектов.

        Args:
            model (Model): Объект Model для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id (str): Идентификатор тенанта (опционально)
        """
        await self.try_set_version_to_created_obj(model, create, model.tenant_id, model.name)
        self.update_obj_version(model.labels, model.timestamp, model.user, model.version)  # type: ignore

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Получает последнюю версию Model-объекта.

        Args:
            tenant_id (str): Идентификатор тенанта
            name (str): Имя Model-объекта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        ModelHistory = Model.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(ModelHistory.version)).where(ModelHistory.tenant_id == tenant_id, ModelHistory.name == name)  # type: ignore
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def save_history(
        self,
        model: Model,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
    ) -> None:
        """
        Сохраняет историческую запись для Model-объекта.

        Args:
            model (Model): Объект Model для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(model, edit_model):
            return None
        logger.info("Model %s modified or deleted. Saving history.", model.name)
        await self.copy_obj_to_history(model.labels, deleted)
        await self.copy_obj_to_history(model, deleted)
        self.update_obj_version(model, datetime.datetime.now(datetime.UTC), None, model.version + 1)  # type: ignore
        await self.session.flush()
        logger.debug("Model %s saved with version %s", model.name, model.version - 1)  # type: ignore
        return None
