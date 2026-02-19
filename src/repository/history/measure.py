"""
Модуль реализует репозиторий для работы с историей показателей (Measure).
Обеспечивает функциональность проверки изменений, обновления версий, получения информации из истории и сохранения изменений.
"""

import datetime
from typing import Optional, Sequence

from py_common_lib.logger import EPMPYLogger
from sqlalchemy import func, select

from src.db.measure import Measure, MeasureModelRelation
from src.pkg.history_meta.history_meta import obj_attr_changed
from src.repository.history.base_history import BaseHistoryRepository

logger = EPMPYLogger(__name__)


class MeasureHistoryRepository(BaseHistoryRepository):
    """
    Репозиторий для управления историей изменений показателей (Measure).
    Расширяет базовый класс BaseHistoryRepository, добавляя специфическую логику для Measure-объектов.

    Обрабатывает:
    - Проверку изменений в структуре Measure
    - Обновление версий связанных объектов
    - Получение последней версии из истории
    - Сохранение изменений в историческую таблицу
    """

    def is_modified(self, measure: Measure, edit_model: Optional[dict] = None) -> bool:
        """
        Проверяет, были ли внесены изменения в Measure-объект.

        Args:
            measure (Measure): Объект Measure для проверки
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования

        Returns:
            bool: True, если были изменения, иначе False
        """
        if not edit_model:
            return False
        current_models = {model.name for model in measure.models}

        edit_filter = edit_model.get("filter") is not None
        edit_labels = edit_model.get("labels") is not None
        models = edit_model.get("models")

        edit_models = False
        if models and set(models) != current_models:
            edit_models = True

        edited_fields = False
        for field_name in list(measure.__class__.__versioned__["check_modified_fields"]):
            edited_fields |= obj_attr_changed(measure, field_name, edit_model.get(field_name))

        unit_of_measure_edit = False
        unit_of_measure = edit_model.get("unit_of_measure")
        if isinstance(unit_of_measure, str):
            unit_of_measure_edit = measure.dimension is None or measure.dimension.name != unit_of_measure
        elif isinstance(unit_of_measure, dict):
            unit_of_measure_edit = (
                measure.dimension is None
                or measure.dimension.name != unit_of_measure["dimension_id"]
                or measure.dimension_value != unit_of_measure["dimension_value"]
            )

        return edit_models or edited_fields or unit_of_measure_edit or edit_filter or edit_labels

    async def update_version(
        self,
        measure: Measure,
        create: bool = False,
        forced_version: int | None = None,
        forced_timestamp: datetime.datetime | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """
        Обновляет версию Measure-объекта и всех связанных с ним объектов.

        Args:
            measure (DataStorage): Объект Measure для обновления
            create (bool): Флаг создания нового объекта (по умолчанию False)
            forced_version (Optiona[int]): Принудительная версия (опционально)
            forced_timestamp(Any): Принудительное время (опционально)
            tenant_id (str): Идентификатор тенанта
        """
        await self.try_set_version_to_created_obj(measure, create, measure.tenant_id, measure.name)
        self.update_obj_version(measure.labels, measure.timestamp, measure.user, measure.version)  # type: ignore
        self.update_obj_version(measure.filter, measure.timestamp, measure.user, measure.version)  # type: ignore
        model_relations = await self.get_measure_model_relation(measure)
        self.update_obj_version(model_relations, measure.timestamp, measure.user, measure.version)  # type: ignore

    async def get_last_version(self, tenant_id: str, name: str) -> int:
        """
        Получает последнюю версию Measure-объекта.

        Args:
            tenant_id (str): Идентификатор тенанта
            name (str): Имя Measure-объекта

        Returns:
            int: Номер последней версии (0, если объект не найден)
        """
        MeasureHistory = Measure.__history_mapper__.class_  # type: ignore
        result = await self.session.execute(
            select(func.max(MeasureHistory.version)).where(  # type: ignore
                MeasureHistory.tenant_id == tenant_id, MeasureHistory.name == name
            )
        )
        version = result.scalar()
        if version is None:
            return 0
        return version

    async def get_measure_model_relation(self, measure: Measure) -> Sequence[MeasureModelRelation]:
        """
        Получает все отношения с моделями для указанного Measure-объекта.

        Args:
            measure: Measure-объект для получения отношений

        Returns:
            Sequence[MeasureModelRelation]: Последовательность объектов CompositeModelRelation
        """
        measure_models = (
            (
                await self.session.execute(
                    select(MeasureModelRelation).where(MeasureModelRelation.measure_id == measure.id)
                )
            )
            .scalars()
            .all()
        )
        return measure_models

    async def save_history(
        self,
        measure: Measure,
        edit_model: Optional[dict] = None,
        deleted: bool = False,
        forced: bool = False,
    ) -> None:
        """
        Сохраняет историческую запись для Measure-объекта.

        Args:
            measure (Measure): Объект Measure для сохранения
            edit_model (Optional[dict]): Необязательный словарь с данными редактирования
            deleted (bool): Флаг удаления объекта (по умолчанию False)
            forced (bool): Принудительное сохранение (даже без изменений)
            forced_version (Optional[int]): Принудительная версия (опционально)
        """
        if not deleted and not forced and not self.is_modified(measure, edit_model):
            return None
        logger.info("Measure %s modified or deleted. Saving history.", measure.name)
        await self.copy_obj_to_history(measure.labels, deleted)
        await self.copy_obj_to_history(measure.filter, deleted)
        model_relations = await self.get_measure_model_relation(measure)
        await self.copy_obj_to_history(model_relations, deleted)
        await self.copy_obj_to_history(measure, deleted)
        self.update_obj_version(measure, datetime.datetime.now(datetime.UTC), None, measure.version + 1)  # type: ignore
        await self.session.flush()
        logger.info("Measure %s saved with version %s", measure.name, measure.version - 1)  # type: ignore
        return None
