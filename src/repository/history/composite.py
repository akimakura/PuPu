"""
Модуль реализует репозиторий для работы с историей композитов (Composite).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.composite import Composite, CompositeModelRelation
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.history.base_history import BaseHistoryRepository
from src.repository.history.utils import get_database_object_relations

logger = EPMPYLogger(__name__)


class CompositeHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей изменений композитов (Composite).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для Composite-объектов.

    Обрабатывает:
    - Проверку изменений в структуре Composite
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def is_modified(self, composite: Composite, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в Composite-объект.

        Args:
            composite (Composite): Объект Composite для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        edited_dso_fields = edit_model.get("fields") is not None
        edited_labels = edit_model.get("labels") is not None
        edited_labels = edit_model.get("link_fields") is not None
        edited_labels = edit_model.get("datasources") is not None
        edited_model_fields = False
        for field_name in list(composite.__class__.__versioned__["check_modified_fields"]):
            edited_model_fields |= obj_attr_changed(composite, field_name, edit_model.get(field_name))
        return edited_dso_fields or edited_labels or edited_model_fields

    async def update_version(
        self,
        composite: Composite,
        create: bool = False,
        forced_version: Optional[int] = None,
        forced_timestamp: Any = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию Composite-объекта и всех связанных с ним объектов.

        Args:
            composite (Composite): Объект Composite для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id (str | None): Идентификатор тенанта (опционально)
        """
        await self.try_set_version_to_created_obj(
            composite,
            create,
            composite.tenant_id,
            composite.name,
            forced_version=forced_version,
            forced_timestamp=forced_timestamp,
        )
        new_version = composite.version if forced_version is None else forced_version  # type: ignore
        new_timestamp = composite.timestamp if forced_timestamp is None else forced_timestamp
        self.update_obj_version(composite.labels, new_timestamp, composite.user, new_version)
        self.update_obj_version(composite.fields, new_timestamp, composite.user, new_version)
        for datastorage_field in composite.fields:
            self.update_obj_version(datastorage_field.labels, new_timestamp, composite.user, new_version)
            self.update_obj_version(datastorage_field.datasource_links, new_timestamp, composite.user, new_version)
            if datastorage_field.any_field is not None:
                self.update_obj_version(datastorage_field.any_field, new_timestamp, composite.user, new_version)
                self.update_obj_version(datastorage_field.any_field.labels, new_timestamp, composite.user, new_version)
        self.update_obj_version(composite.datasources, new_timestamp, composite.user, new_version)
        self.update_obj_version(composite.link_fields, new_timestamp, composite.user, new_version)
        model_relations = await self.get_composite_model_relation(composite)
        self.update_obj_version(model_relations, new_timestamp, composite.user, new_version)
        self.update_obj_version(composite.database_objects, new_timestamp, composite.user, new_version)
        db_object_model_relations = await get_database_object_relations(self.session, composite.database_objects)
        self.update_obj_version(db_object_model_relations, new_timestamp, composite.user, new_version)
        for db_object in composite.database_objects:
            self.update_obj_version(db_object.specific_attributes, new_timestamp, composite.user, new_version)

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Получает последнюю версию Composite-объекта.

        Args:
            tenant_id (str): Идентификатор тенанта
            name (str): Имя Composite-объекта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        CompositeHistory = Composite.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(CompositeHistory.version)).where(  # type: ignore
                CompositeHistory.tenant_id == tenant_id, CompositeHistory.name == name
            )
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def get_composite_model_relation(self, composite: Composite) -> Sequence[CompositeModelRelation]:
        """
        Получает все отношения с моделями для указанного Composite-объекта.

        Args:
            composite: Composite-объект для получения отношений

        Returns:
            Sequence[CompositeModelRelation]: Последовательность объектов CompositeModelRelation
        """
        measure_models = (
            (
                await self.session.execute(
                    select(CompositeModelRelation).where(CompositeModelRelation.composite_id == composite.id)
                )
            )
            .scalars()
            .all()
        )
        return measure_models

    async def save_history(
        self,
        composite: Composite,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
        forced_version: Optional[int] = None,
    ) -> None:
        """
        Сохраняет историческую запись для Composite-объекта.

        Args:
            composite (Composite): Объект Composite для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(composite, edit_model):
            return None
        logger.info("Composite %s modified or deleted. Saving history.", composite.name)
        await self.copy_obj_to_history(composite.labels, deleted)
        for composite_field in composite.fields:
            await self.copy_obj_to_history(composite_field.datasource_links, deleted)
            await self.copy_obj_to_history(composite_field.labels, deleted)
            if composite_field.any_field is not None:
                await self.copy_obj_to_history(composite_field.any_field, deleted)
                await self.copy_obj_to_history(composite_field.any_field.labels, deleted)
        await self.copy_obj_to_history(composite.fields, deleted)
        await self.copy_obj_to_history(composite.datasources, deleted)
        await self.copy_obj_to_history(composite.link_fields, deleted)
        model_relations = await self.get_composite_model_relation(composite)
        await self.copy_obj_to_history(model_relations, deleted)
        await self.copy_obj_to_history(composite.database_objects, deleted)
        db_object_model_relations = await get_database_object_relations(self.session, composite.database_objects)
        await self.copy_obj_to_history(db_object_model_relations, deleted)
        for db_object in composite.database_objects:
            await self.copy_obj_to_history(db_object.specific_attributes, deleted)
        await self.copy_obj_to_history(composite, deleted)
        new_version = composite.version + 1 if forced_version is None else forced_version  # type: ignore
        self.update_obj_version(composite, datetime.datetime.now(datetime.UTC), None, new_version)
        await self.session.flush()
        logger.info("Composite %s saved with version %s", composite.name, new_version - 1)
        return None
