"""
Репозиторий для иерархий признаков
"""

import logging
from collections.abc import Sequence
from typing import Optional

from py_common_lib.utils import timeit
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Delete, Select

from src.db import HierarchyBaseDimension, HierarchyLabel, HierarchyMeta, HierarchyModelRelation
from src.db.dimension import Dimension
from src.db.model import Model
from src.models.hierarchy import HierarchyCreateRequest
from src.models.request_params import Pagination

logger = logging.getLogger(__name__)


class HierarchyRepository:

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "HierarchyRepository":
        return cls(session)

    @staticmethod
    def _apply_pagination(
        query: Select[tuple[HierarchyMeta]], pagination: Optional[Pagination]
    ) -> Select[tuple[HierarchyMeta]]:
        """
        Применяет пагинацию к запросу.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            pagination (Optional[Pagination]): Параметры пагинации.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с применением пагинации.
        """
        if not pagination:
            return query
        return query.offset(pagination.offset).limit(pagination.limit)

    @staticmethod
    def _statement_get_multi() -> Select[tuple[HierarchyMeta]]:
        """
        Базовый запрос для получения нескольких иерархий.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос для выбора иерархий.
        """
        return select(HierarchyMeta)

    @staticmethod
    def _apply_joined_model(query: Select[tuple[HierarchyMeta]]) -> Select[tuple[HierarchyMeta]]:
        """
        Присоединяет модели к запросу иерархий.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с присоединённой моделью.
        """
        return query.join(
            HierarchyModelRelation, HierarchyMeta.id == HierarchyModelRelation.hierarchy_id, isouter=True
        ).join(Model, Model.id == HierarchyModelRelation.model_id)

    @staticmethod
    def _apply_joined_dimensions(query: Select[tuple[HierarchyMeta]]) -> Select[tuple[HierarchyMeta]]:
        """
        Присоединяет базовые измерения к запросу иерархий.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с присоединёнными измерениями.
        """
        return query.join(
            HierarchyBaseDimension, HierarchyMeta.id == HierarchyBaseDimension.hierarchy_id, isouter=True
        ).join(Dimension, Dimension.id == HierarchyBaseDimension.dimension_id)

    @staticmethod
    def _apply_filter_by_model_name(
        query: Select[tuple[HierarchyMeta]], model_name: Optional[str]
    ) -> Select[tuple[HierarchyMeta]]:
        """
        Фильтрует иерархии по названию модели.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            model_name (Optional[str]): Название модели.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с фильтрацией по модели.
        """
        if model_name is None:
            return query
        return query.where(Model.name == model_name)

    @staticmethod
    def _apply_filter_by_tenant_id(
        query: Select[tuple[HierarchyMeta]], tenant_id: Optional[str]
    ) -> Select[tuple[HierarchyMeta]]:
        """
        Фильтрует иерархии по названию модели.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            model_name (Optional[str]): Название модели.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с фильтрацией по модели.
        """
        if tenant_id is None:
            return query
        return query.where(HierarchyMeta.base_dimensions.any(Dimension.tenant_id == tenant_id))

    @staticmethod
    def _apply_filter_by_name(query: Select[tuple[HierarchyMeta]], name: str) -> Select[tuple[HierarchyMeta]]:
        """
        Фильтрует иерархии по названию.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            name (str): Название иерархии.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с фильтрацией по названию.
        """
        return query.where(HierarchyMeta.name == name)

    @staticmethod
    def _apply_filter_by_names(query: Select[tuple[HierarchyMeta]], names: list[str]) -> Select[tuple[HierarchyMeta]]:
        """
        Фильтрует иерархии по множеству названий.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            names (List[str]): Множество названий иерархий.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с фильтрацией по названиям.
        """
        return query.where(HierarchyMeta.name.in_(names))

    @staticmethod
    def _apply_joined_labels(query: Select[tuple[HierarchyMeta]]) -> Select[tuple[HierarchyMeta]]:
        """
        Присоединяет метки к запросу иерархий.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с присоединёнными метками.
        """
        return query.join(HierarchyLabel, isouter=True)

    @staticmethod
    def _apply_filter_by_dimension_names(
        query: Select[tuple[HierarchyMeta]], dimension_names: list[str] | None
    ) -> Select[tuple[HierarchyMeta]]:
        """
        Фильтрует иерархии по названиям базовых измерений.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            dimension_names (List[str]): Множество названий измерений.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с фильтрацией по измерениям.
        """
        if dimension_names is None:
            return query
        return query.where(HierarchyMeta.base_dimensions.any(Dimension.name.in_(dimension_names)))

    @staticmethod
    def _apply_filter_by_hierarchy_names(
        query: Select[tuple[HierarchyMeta]], hierarchy_names: list[str]
    ) -> Select[tuple[HierarchyMeta]]:
        """
        Фильтрует иерархии по множеству названий.

        Args:
            query (select[tuple[HierarchyMeta]]): Основной запрос.
            hierarchy_names (List[str]): Множество названий иерархий.

        Returns:
            select[tuple[HierarchyMeta]]: Запрос с фильтрацией по названиям иерархий.
        """
        if not hierarchy_names:
            return query
        return query.where(HierarchyMeta.name.in_(hierarchy_names))

    async def add_hierarchy_to_model(self, model_id: int, hierarchy_id: int) -> None:
        """
        Добавляет иерархию к модели.

        Args:
            model_id (int): Идентификатор модели.
            hierarchy_id (int): Идентификатор иерархии.
        """
        orm_model = HierarchyModelRelation(
            hierarchy_id=hierarchy_id,
            model_id=model_id,
        )
        self.session.add(orm_model)
        await self.session.flush([orm_model])

    @timeit
    async def get_list(
        self,
        tenant_id: str | None,
        model_name: str | None,
        hierarchy_names: list[str],
        dimension_names: list[str] | None,
        pagination: Optional[Pagination] = None,
    ) -> Sequence[HierarchyMeta]:
        """
        Получить список иерархий по указанным параметрам.

        Args:
            model_name (str): Название модели, в рамках которой запрашиваются иерархии.
            hierarchy_names (list[str]): Список названий иерархий для фильтрации результата.
            dimension_names (list[str]): Список названий измерений, связанных с искомыми иерархиями.
            pagination (Optional[Pagination], optional): Параметры постраничной разбивки результатов. По умолчанию None.

        Returns:
            Sequence[HierarchyMeta]: Последовательность метаданных найденных иерархий.
        """
        query = self._statement_get_multi()
        query = self._apply_joined_model(query)
        query = self._apply_joined_dimensions(query)
        query = self._apply_joined_labels(query)
        query = self._apply_filter_by_tenant_id(query, tenant_id)
        query = self._apply_filter_by_model_name(query, model_name)
        # need optimization
        query = self._apply_filter_by_dimension_names(query, dimension_names)
        query = self._apply_filter_by_hierarchy_names(query, hierarchy_names)
        result = (await self.session.execute(query)).scalars().unique().all()

        return result

    @timeit
    async def get_list_by_names(
        self, model_name: str, names: list[str], pagination: Optional[Pagination] = None
    ) -> Sequence[HierarchyMeta]:
        """
        Возвращает список иерархий по конкретным именам.

        Args:
            model_name (str): Название модели, в рамках которой производится поиск иерархий.
            names (list[str]): Список имен иерархий, которые нужно найти.
            pagination (Optional[Pagination], optional): Параметры постраничного вывода результатов. По умолчанию None.

        Returns:
            Sequence[HierarchyMeta]: Последовательность метаданных иерархий, соответствующих переданным именам.
        """
        query = self._statement_get_multi()
        query = self._apply_joined_model(query)

        query = self._apply_filter_by_model_name(query, model_name)
        query = self._apply_filter_by_names(query, names)
        query = self._apply_pagination(query, pagination)
        return (await self.session.execute(query)).scalars().unique().all()

    @timeit
    async def get_by_name(self, model_name: str, name: str) -> list[HierarchyMeta]:
        """
        Возвращает список иерархий по указанному имени.

        Args:
            model_name (str): Название модели, в рамках которой выполняется поиск иерархий.
            name (str): Имя иерархии, которую нужно найти.

        Returns:
            list[HierarchyMeta]: Список метаданных иерархий, соответствующих переданному имени.
        """
        query = self._statement_get_multi()
        query = self._apply_joined_model(query)
        query = self._apply_joined_dimensions(query)
        query = self._apply_joined_labels(query)

        query = self._apply_filter_by_model_name(query, model_name)
        query = self._apply_filter_by_name(query, name)
        result = (await self.session.execute(query)).scalars().unique().all()
        return list(result)

    @timeit
    async def get_base_dimension_names_by_hierarchy_id(self, hierarchy_id: int) -> list[tuple[str, bool]]:
        """
        Возвращает список базовых измерений для указанной иерархии.

        Args:
            hierarchy_id (int): Идентификатор иерархии, для которой нужно получить базовые измерения.

        Returns:
            list[tuple[str, bool]]: Список кортежей, где первый элемент — имя базового измерения, второй —
                                   булево значение, указывающее, является ли данное измерение основным.
        """
        query = (
            select(Dimension.name)
            .select_from(HierarchyBaseDimension)
            .join(HierarchyMeta)
            .join(Dimension)
            .where(HierarchyMeta.id == hierarchy_id)
            .add_columns(HierarchyBaseDimension.is_base.label("is_base"))
        )
        return list((await self.session.execute(query)).unique().all())  # type: ignore[arg-type]

    @timeit
    async def is_hierarchy_name_has_base_dimension(self, hierarchy_name: str, base_dimension_id: int) -> bool:
        """
        Проверяет наличие указанного базового измерения у данной иерархии.

        Args:
            hierarchy_name (str): Имя иерархии, для которой проверяется наличие базового измерения.
            base_dimension_id (int): ID базового измерения, наличие которого проверяется.

        Returns:
            bool: True, если указанное базовое измерение присутствует у иерархии, иначе False.
        """

        query = (
            select(HierarchyBaseDimension)
            .join(HierarchyMeta)
            .where(HierarchyMeta.name == hierarchy_name)
            .where(HierarchyBaseDimension.dimension_id == base_dimension_id)
            .where(HierarchyBaseDimension.is_base == True)  # noqa E712
        )
        return bool((await self.session.execute(query)).scalars().one_or_none())

    async def get_hierarchy_base_dimension_relations(self, hierarchy_id: int) -> list[HierarchyBaseDimension]:
        """
        Возвращает список базовых измерений, связанных с указанной иерархией.

        Метод извлекает все отношения базовых измерений для заданной иерархии,
        которые определяют её основную структуру (например, «Контрагент», «Номенклатура» и т.д.).

        Args:
            hierarchy_id (int): Уникальный идентификатор иерархии.

        Returns:
            list[HierarchyBaseDimension]: Список объектов, представляющих связи с базовыми измерениями.
            Если такие связи отсутствуют, возвращается пустой список.
        """

        query = select(HierarchyBaseDimension).where(HierarchyBaseDimension.hierarchy_id == hierarchy_id)
        return list((await self.session.execute(query)).scalars().all())

    async def get_hierarchy_model_relations(self, hierarchy_id: int) -> list[HierarchyModelRelation]:
        """
        Возвращает список всех связей между указанной иерархией и моделями.

        Метод извлекает все записи отношений для заданной иерархии, показывая, с какими
        моделями она ассоциирована и как настроена эта ассоциация (например, дополнительные измерения, флаги и т.д.).

        Args:
            hierarchy_id (int): Уникальный идентификатор иерархии.

        Returns:
            list[HierarchyModelRelation]: Список объектов, представляющих связи между иерархией и моделями.
            Если связи отсутствуют, возвращается пустой список.
        """

        query = select(HierarchyModelRelation).where(HierarchyModelRelation.hierarchy_id == hierarchy_id)
        return list((await self.session.execute(query)).scalars().all())

    @timeit
    async def get_hierarchy_base_dimensions_by_hierarchy_id(self, hierarchy_id: int) -> list[HierarchyBaseDimension]:
        """
        Возвращает список базовых измерений для заданной иерархии.

        Args:
            hierarchy_id (int): Уникальный идентификатор иерархии, для которой возвращаются базовые измерения.

        Returns:
            list[HierarchyBaseDimension]: Список объектов HierarchyBaseDimension, представляющих базовые измерения иерархии.
        """
        query = (
            select(HierarchyBaseDimension)
            .join(Dimension)
            .where(HierarchyBaseDimension.hierarchy_id == hierarchy_id)
            .add_columns(Dimension.name.label("dimension_name"))
        )
        raw_result = (await self.session.execute(query)).all()
        result = []
        for row in raw_result:
            base_dimension, dimension_name = row
            base_dimension.dimension_name = dimension_name
            result.append(base_dimension)
        return result

    async def _get_hierarchy_model_relation(self, hierarchy_id: int, model_name: str) -> HierarchyModelRelation:
        """
        Возвращает объект связи между иерархией и моделью по заданному ID иерархии и имени модели.

        Метод извлекает запись отношения из хранилища, которая определяет, как иерархия
        связана с конкретной моделью данных (например, какие дополнительные измерения используются).

        Args:
            hierarchy_id (int): Уникальный идентификатор иерархии.
            model_name (str): Имя модели, с которой проверяется связь.

        Returns:
            HierarchyModelRelation: Объект, представляющий связь между иерархией и моделью,
            содержащий, например, настройки отображения, дополнительные измерения и флаги синхронизации.

        Raises:
            RelationNotFound: Если связь между указанной иерархией и моделью не найдена.
        """
        query = (
            select(HierarchyModelRelation)
            .join(Model)
            .where(HierarchyModelRelation.hierarchy_id == hierarchy_id)
            .where(Model.name == model_name)
        )
        hierarchy_model_relation = (await self.session.execute(query)).scalars().first()
        if not hierarchy_model_relation:
            raise NoResultFound
        return hierarchy_model_relation

    async def _delete_hierarchy_labels(self, hierarchy_id: int) -> None:
        """
        Удаляет метки (labels), относящиеся к указанной иерархии.

        Args:
            hierarchy_id (int): Идентификатор иерархии, чьи метки удаляются.
        """
        query = Delete(HierarchyLabel).where(HierarchyLabel.hierarchy_id == hierarchy_id)
        await self.session.execute(query)

    async def _delete_hierarchy_base_dimensions(self, hierarchy_id: int) -> None:
        """
        Удаляет базовые измерения, принадлежащие указанной иерархии.

        Args:
            hierarchy_id (int): Идентификатор иерархии, чьё основное измерение удаляется.
        """
        query = Delete(HierarchyBaseDimension).where(HierarchyBaseDimension.hierarchy_id == hierarchy_id)
        await self.session.execute(query)

    async def _delete_hierarchy_model(self, hierarchy_id: int, model_name: str) -> None:
        """
        Удаляет модель иерархии, связанную с указанным идентификатором.

        Args:
            hierarchy_id (int): Идентификатор иерархии, чья модель удаляется.
            model_name (str): Название модели, связанной с иерархией.
        """

        hierarchy_model_relation = await self._get_hierarchy_model_relation(hierarchy_id, model_name)
        await self.session.delete(hierarchy_model_relation)

    @timeit
    async def delete_by_id(self, hierarchy_id: int, model_name: str) -> None:
        """
        Полное удаление иерархии по её идентификатору.

        Сначала удаляются метки, базовые измерения и модель иерархии, после чего сама иерархия удаляется окончательно.

        Args:
            hierarchy_id (int): Идентификатор иерархии, подлежащей полному удалению.
        """
        await self._delete_hierarchy_labels(hierarchy_id)
        await self._delete_hierarchy_base_dimensions(hierarchy_id)
        await self._delete_hierarchy_model(hierarchy_id, model_name)
        await self.session.flush()

    async def create_by_schema(self, hierarchy: HierarchyCreateRequest, base_dimension_name: str) -> HierarchyMeta:
        """
        Создаёт новую иерархию на основании схемы и базовой структуры хранилища данных.

        Args:
            hierarchy (HierarchyCreateRequest): Запрос на создание иерархии с необходимыми параметрами.
            base_dimension_name (str): Базовое имя измерения, используемое для формирования путей хранения данных.

        Returns:
            HierarchyMeta: Объект метаинформации созданной иерархии.
        """
        hierarchy_meta = HierarchyMeta(
            name=hierarchy.name,
            default_expansion=hierarchy.default_expansion,
            structure_type=hierarchy.structure_type,
            time_dependency_type=hierarchy.time_dependency_type,
            aggregation_type=hierarchy.aggregation_type,
            default_hierarchy=hierarchy.default_hierarchy,
            is_time_dependent=hierarchy.is_time_dependent,
            is_versioned=hierarchy.is_versioned,
            input_on_nodes=hierarchy.input_on_nodes,
            data_storage_versions=f"{base_dimension_name}_versions",
            data_storage_text_versions=f"{base_dimension_name}_textversions",
            data_storage_nodes=f"{base_dimension_name}_nodes",
            data_storage_text_nodes=f"{base_dimension_name}_textnodes",
        )
        self.session.add(hierarchy_meta)
        await self.session.flush()
        await self.session.refresh(hierarchy_meta)
        return hierarchy_meta

    async def check_whether_hierarchy_has_base_dimension(self, hierarchy_id: int) -> bool:
        """
        Проверяет, существует ли у иерархии хотя бы одно базовое измерение.

        Args:
            hierarchy_id (int): Идентификатор иерархии для проверки.

        Returns:
            bool: True, если есть хотя бы одно базовое измерение, иначе False.
        """
        query = (
            select(HierarchyBaseDimension)
            .where(HierarchyBaseDimension.hierarchy_id == hierarchy_id)
            .where(HierarchyBaseDimension.is_base == True)  # noqa E712
        )
        result = (await self.session.execute(query)).scalars().one_or_none()
        return bool(result)

    async def set_owner_model(self, hierarchies: list[HierarchyMeta], model: Model) -> None:
        """Обновляет состояние владельца модели."""
        hierarchies_ids = [hierarchy.id for hierarchy in hierarchies]
        await self.session.execute(
            update(HierarchyModelRelation)
            .where(
                HierarchyModelRelation.hierarchy_id.in_(hierarchies_ids),
            )
            .values({"is_owner": False})
        )
        await self.session.execute(
            update(HierarchyModelRelation)
            .where(
                HierarchyModelRelation.hierarchy_id.in_(hierarchies_ids),
                HierarchyModelRelation.model_id == model.id,
            )
            .values({"is_owner": True})
        )
