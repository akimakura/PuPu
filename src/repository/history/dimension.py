"""
Модуль реализует репозиторий для работы с историей измерений (Dimension).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Any, Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.dimension import Dimension, DimensionModelRelation
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.history.base_history import BaseHistoryRepository
from src.repository.history.data_storage import DataStorageHistoryRepository

logger = EPMPYLogger(__name__)


class DimensionHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей изменений измерений (Dimension).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для DataStorage-объектов.

    Обрабатывает:
    - Проверку изменений в структуре Dimension
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def __init__(self, session: AsyncSession) -> None:
        self.data_storage_history_repository = DataStorageHistoryRepository(session)
        super().__init__(session)

    def is_modified(self, dimension: Dimension, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в Dimension-объект.

        Args:
            dimension (Dimension): Объект Dimension для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        edited_attributes = edit_model.get("attributes") is not None
        edited_labels = edit_model.get("labels") is not None
        edited_prompt = edit_model.get("prompt") is not None
        edited_texts = edit_model.get("texts") is not None
        edited_model_fields = False
        for field_name in list(dimension.__class__.__versioned__["check_modified_fields"]):
            edited_model_fields |= obj_attr_changed(dimension, field_name, edit_model.get(field_name))
        return edited_attributes or edited_labels or edited_model_fields or edited_prompt or edited_texts

    async def update_version(
        self,
        dimension: Dimension,
        create: bool = False,
        forced_version: Optional[int] = None,
        forced_timestamp: Any = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию Dimension-объекта и всех связанных с ним объектов.

        Args:
            dimension (Dimension): Объект Dimension для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id (str): Идентификатор тенанта (опционально)
        """
        await self.try_set_version_to_created_obj(
            dimension,
            create,
            dimension.tenant_id,
            dimension.name,
            forced_version=forced_version,
            forced_timestamp=forced_timestamp,
        )
        new_version = dimension.version if forced_version is None else forced_version  # type: ignore
        new_timestamp = dimension.timestamp if forced_timestamp is None else forced_timestamp
        self.update_obj_version(dimension.labels, new_timestamp, dimension.user, new_version)
        self.update_obj_version(dimension.attributes, new_timestamp, dimension.user, new_version)
        self.update_obj_version(dimension.texts, new_timestamp, dimension.user, new_version)
        self.update_obj_version(dimension.pv_dictionary, new_timestamp, dimension.user, new_version)
        self.update_obj_version(dimension.prompt, new_timestamp, dimension.user, new_version)
        for attribute in dimension.attributes:
            self.update_obj_version(attribute.labels, new_timestamp, dimension.user, new_version)
            if attribute.any_field_attribute is not None:
                self.update_obj_version(attribute.any_field_attribute, new_timestamp, dimension.user, new_version)
                self.update_obj_version(
                    attribute.any_field_attribute.labels, new_timestamp, dimension.user, new_version
                )
        model_relations = await self.get_dimension_model_relation(dimension)
        self.update_obj_version(model_relations, new_timestamp, dimension.user, new_version)
        for table in [dimension.text_table, dimension.values_table, dimension.attributes_table]:
            if table:
                table.version = new_version  # type: ignore
                table.timestamp = new_timestamp
                table.user = dimension.user
                await self.data_storage_history_repository.update_version(
                    table,
                    forced_timestamp=new_timestamp,
                    forced_version=new_version,
                )

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Получает последнюю версию Dimension-объекта.

        Args:
            tenant_id (str): Идентификатор тенанта
            name (str): Имя Dimension-объекта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        DimensionHistory = Dimension.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(DimensionHistory.version)).where(
                DimensionHistory.tenant_id == tenant_id, DimensionHistory.name == name
            )
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def get_dimension_model_relation(self, dimension: Dimension) -> Sequence[DimensionModelRelation]:
        """
        Получает все отношения с моделями для указанного Dimension-объекта.

        Args:
            dimension: Dimension-объект для получения отношений

        Returns:
            Sequence[DimensionModelRelation]: Последовательность объектов CompositeModelRelation
        """
        measure_models = (
            (
                await self.session.execute(
                    select(DimensionModelRelation).where(DimensionModelRelation.dimension_id == dimension.id)
                )
            )
            .scalars()
            .all()
        )
        return measure_models

    async def save_history(
        self,
        dimension: Dimension,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
        forced_version: Optional[int] = None,
    ) -> None:
        """
        Сохраняет историческую запись для Dimension-объекта.

        Args:
            dimension (Dimension): Объект Dimension для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(dimension, edit_model):
            return None

        logger.info("Dimension %s modified or deleted. Saving history.", dimension.name)
        await self.copy_obj_to_history(dimension.labels, deleted)
        await self.copy_obj_to_history(dimension.texts, deleted)
        await self.copy_obj_to_history(dimension.pv_dictionary, deleted)
        await self.copy_obj_to_history(dimension.prompt, deleted)

        for attribute in dimension.attributes:
            await self.copy_obj_to_history(attribute.labels, deleted)
            if attribute.any_field_attribute is not None:
                await self.copy_obj_to_history(attribute.any_field_attribute, deleted)
                await self.copy_obj_to_history(attribute.any_field_attribute.labels, deleted)
        await self.copy_obj_to_history(dimension.attributes, deleted)

        model_relations = await self.get_dimension_model_relation(dimension)
        await self.copy_obj_to_history(model_relations, deleted)

        new_version = forced_version or self.get_new_version(dimension)  # type: ignore

        for table in [dimension.text_table, dimension.values_table, dimension.attributes_table]:
            if table:
                await self.data_storage_history_repository.save_history(
                    table,
                    forced=True,
                    forced_version=new_version,
                    deleted=deleted,
                )

        await self.copy_obj_to_history(dimension, deleted)
        self.update_obj_version(dimension, datetime.datetime.now(datetime.UTC), None, new_version)

        await self.session.flush()
        logger.info("Dimension %s saved with version %s", dimension.name, new_version - 1)
        return None
