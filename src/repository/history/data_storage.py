"""
Модуль реализует репозиторий для работы с историей хранилищ данных (Datastorage).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.data_storage import DataStorage, DataStorageModelRelation
from src.db.database_object import DatabaseObjectRelation
from src.models.tenant import SemanticObjectsTypeEnum
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.history.base_history import BaseHistoryRepository
from src.repository.history.utils import get_database_object_relations

logger = EPMPYLogger(__name__)


class DataStorageHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей изменений хранилищ данных (DataStorage).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для DataStorage-объектов.

    Обрабатывает:
    - Проверку изменений в структуре DataStorage
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def is_modified(self, data_storage: DataStorage, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в DataStorage-объект.

        Args:
            data_storage (DataStorage): Объект DataStorage для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        edited_dso_fields = edit_model.get("fields") is not None
        edited_labels = edit_model.get("labels") is not None
        edited_model_fields = False
        for field_name in list(data_storage.__class__.__versioned__["check_modified_fields"]):
            edited_model_fields |= obj_attr_changed(data_storage, field_name, edit_model.get(field_name))
        return edited_dso_fields or edited_labels or edited_model_fields

    async def update_version(
        self,
        datastorage: DataStorage,
        create: bool = False,
        forced_version: Optional[int] = None,
        forced_timestamp: Any = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию DataStorage-объекта и всех связанных с ним объектов.

        Args:
            datastorage (DataStorage): Объект DataStorage для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id (str | None): Идентификатор тенанта (опционально)
        """
        await self.try_set_version_to_created_obj(
            datastorage,
            create,
            datastorage.tenant_id,
            datastorage.name,
            forced_version=forced_version,
            forced_timestamp=forced_timestamp,
        )

        new_version = datastorage.version if forced_version is None else forced_version  # type: ignore
        new_timestamp = datastorage.timestamp if forced_timestamp is None else forced_timestamp
        self.update_obj_version(datastorage.labels, new_timestamp, datastorage.user, new_version)

        self.update_obj_version(datastorage.fields, new_timestamp, datastorage.user, new_version)
        for datastorage_field in datastorage.fields:
            self.update_obj_version(datastorage_field.labels, new_timestamp, datastorage.user, new_version)
            if datastorage_field.any_field is not None:
                self.update_obj_version(datastorage_field.any_field, new_timestamp, datastorage.user, new_version)
                self.update_obj_version(
                    datastorage_field.any_field.labels, new_timestamp, datastorage.user, new_version
                )

        model_relations = await self.get_data_storage_model_relation(datastorage)
        self.update_obj_version(model_relations, new_timestamp, datastorage.user, new_version)

        self.update_obj_version(datastorage.database_objects, new_timestamp, datastorage.user, new_version)
        db_object_model_relations = await get_database_object_relations(self.session, datastorage.database_objects)
        self.update_obj_version(db_object_model_relations, new_timestamp, datastorage.user, new_version)
        for db_object in datastorage.database_objects:
            self.update_obj_version(db_object.specific_attributes, new_timestamp, datastorage.user, new_version)

        if datastorage.log_data_storage:
            datastorage.log_data_storage.version = new_version  # type: ignore
            datastorage.log_data_storage.user = datastorage.user
            datastorage.log_data_storage.timestamp = datastorage.timestamp
            await self.update_version(datastorage.log_data_storage, create, new_version, new_timestamp)

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Получает последнюю версию DataStorage-объекта.

        Args:
            tenant_id (str): Идентификатор тенанта
            name (str): Имя DataStorage-объекта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        DataStorageHistory = DataStorage.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(DataStorageHistory.version)).where(  # type: ignore
                DataStorageHistory.tenant_id == tenant_id, DataStorageHistory.name == name
            )
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def get_data_storage_model_relation(self, datastorage: DataStorage) -> Sequence[DataStorageModelRelation]:
        """
        Получает все отношения с моделями для указанного DataStorage-объекта.

        Args:
            datastorage: DataStorage-объект для получения отношений

        Returns:
            Sequence[DataStorageModelRelation]: Последовательность объектов CompositeModelRelation
        """
        measure_models = (
            (
                await self.session.execute(
                    select(DataStorageModelRelation).where(DataStorageModelRelation.data_storage_id == datastorage.id)
                )
            )
            .scalars()
            .all()
        )
        return measure_models

    async def save_history(
        self,
        datastorage: DataStorage,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
        forced_version: Optional[int] = None,
    ) -> None:
        """
        Сохраняет историческую запись для DataStorage-объекта.

        Args:
            datastorage (DataStorage): Объект DataStorage для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(datastorage, edit_model):
            return None
        logger.info("DataStorage %s modified or deleted. Saving history.", datastorage.name)

        await self.copy_obj_to_history(datastorage.labels, deleted)

        for datastorage_field in datastorage.fields:
            await self.copy_obj_to_history(datastorage_field.labels, deleted)
            if datastorage_field.any_field is not None:
                await self.copy_obj_to_history(datastorage_field.any_field, deleted)
                await self.copy_obj_to_history(datastorage_field.any_field.labels, deleted)
        await self.copy_obj_to_history(datastorage.fields, deleted)

        model_relations = await self.get_data_storage_model_relation(datastorage)
        await self.copy_obj_to_history(model_relations, deleted)

        database_object_relations = (
            (
                await self.session.execute(
                    select(DatabaseObjectRelation).where(
                        DatabaseObjectRelation.semantic_object_type == SemanticObjectsTypeEnum.DATA_STORAGE,
                        DatabaseObjectRelation.semantic_object_id == datastorage.id,
                        DatabaseObjectRelation.semantic_object_version == datastorage.version,
                    )
                )
            )
            .scalars()
            .all()
        )
        await self.copy_obj_to_history(database_object_relations, deleted)
        for relation in database_object_relations:
            await self.session.delete(relation)

        await self.copy_obj_to_history(datastorage.database_objects, deleted)
        db_object_model_relations = await get_database_object_relations(self.session, datastorage.database_objects)
        await self.copy_obj_to_history(db_object_model_relations, deleted)
        for db_object in datastorage.database_objects:
            await self.copy_obj_to_history(db_object.specific_attributes, deleted)

        await self.copy_obj_to_history(datastorage, deleted)
        new_version = datastorage.version + 1 if forced_version is None else forced_version  # type: ignore

        if datastorage.log_data_storage:
            await self.save_history(
                datastorage.log_data_storage, deleted=deleted, forced=True, forced_version=new_version
            )

        self.update_obj_version(datastorage, datetime.datetime.now(datetime.UTC), None, new_version)
        await self.session.flush()
        logger.info("Datastorage %s saved with version %s", datastorage.name, new_version - 1)
        return None
