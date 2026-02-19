"""
Репозиторий баз данных.
"""

from typing import Optional, Sequence

from py_common_lib.utils import timeit
from sqlalchemy import Row, and_, or_, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import Composite, DataStorage, Dimension, Measure, Model
from src.db.composite import CompositeLabel, CompositeModelRelation
from src.db.data_storage import DataStorageLabel, DataStorageModelRelation
from src.db.dimension import DimensionLabel, DimensionModelRelation
from src.db.measure import MeasureLabel, MeasureModelRelation
from src.db.tenant import Tenant, TenantLabel
from src.models.request_params import Pagination
from src.models.tenant import (
    BaseModelData,
    SemanticObjects,
    SemanticObjectsTypeEnum,
    Tenant as TenantModel,
    TenantCreateRequest as TenantCreateRequestModel,
    TenantEditRequest as TenantEditRequestModel,
)
from src.repository.history.tenant import TenantHistoryRepository
from src.repository.utils import (
    add_missing_labels,
    convert_labels_list_to_orm,
    get_select_query_with_offset_limit_order,
)


class TenantRepository:

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.tenant_history = TenantHistoryRepository(session)

    async def _get_tenant_orm_by_session(self, name: str) -> Optional[Tenant]:
        """Получить Tenant"""
        result = (
            (
                await self.session.execute(
                    select(Tenant).where(
                        Tenant.name == name,
                    )
                )
            )
            .scalars()
            .one_or_none()
        )
        return result

    async def get_tenant_orm_by_session_with_error(self, name: str) -> Tenant:
        database = await self._get_tenant_orm_by_session(name=name)
        if database:
            return database
        raise NoResultFound(f"""Tenant with name={name} not found.""")

    @timeit
    async def get_list(self, pagination: Optional[Pagination] = None) -> list[TenantModel]:
        """Получить список всех Tenant."""
        query = select(Tenant)
        query = get_select_query_with_offset_limit_order(query, Tenant.name, pagination)
        result = (await self.session.execute(query)).scalars().all()
        return [TenantModel.model_validate(tenant) for tenant in result]

    @timeit
    async def get_by_name(self, name: str) -> TenantModel:
        """Получить Tenant по её имени."""
        result = await self._get_tenant_orm_by_session(name=name)
        if result is None:
            raise NoResultFound(f"Tenant with name={name} not found.")
        return TenantModel.model_validate(result)

    @timeit
    async def delete_by_name(self, name: str) -> None:
        """Удалить Tenant по имени."""
        result = await self._get_tenant_orm_by_session(name=name)
        if result is None:
            raise NoResultFound(f"Tenant with name={name} not found.")
        await self.tenant_history.save_history(result, deleted=True)
        await self.session.delete(result)
        await self.session.commit()

    @timeit
    async def create_by_schema(self, tenant: TenantCreateRequestModel) -> TenantModel:
        """Создать Tenant."""
        tenant_dict = tenant.model_dump(mode="json")
        add_missing_labels(tenant_dict["labels"], tenant.name)
        tenant_dict["labels"] = convert_labels_list_to_orm(labels=tenant_dict["labels"], model=TenantLabel)
        tenant_orm = Tenant(**tenant_dict)
        self.session.add(tenant_orm)
        await self.session.flush()
        await self.tenant_history.update_version(tenant_orm, create=True)
        await self.session.commit()
        return TenantModel.model_validate(tenant_orm)

    @timeit
    async def update_by_name_and_schema(self, name: str, tenant: TenantEditRequestModel) -> TenantModel:
        """Обновить Tenant."""
        tenant_dict = tenant.model_dump(mode="json", exclude_none=True)
        original_tenant = await self._get_tenant_orm_by_session(name=name)
        if original_tenant is None:
            raise NoResultFound(f"Tenant with name={name} not found.")
        await self.tenant_history.save_history(original_tenant, edit_model=tenant_dict)
        if tenant_dict.get("labels") is not None:
            add_missing_labels(tenant_dict["labels"], name)
            original_tenant.labels = convert_labels_list_to_orm(
                tenant_dict.pop("labels"),
                TenantLabel,
            )
        await self.tenant_history.update_version(original_tenant)
        await self.session.commit()
        return TenantModel.model_validate(original_tenant)

    @timeit
    async def get_semantic_objects_by_tenant_and_search_string(
        self,
        tenant_name: str,
        search: str,
        model_name: Optional[str] = None,
        element_type: Optional[SemanticObjectsTypeEnum] = None,
    ) -> dict[str, SemanticObjects]:
        """
        Args:
            tenant_name (str): Имя тенанта.
            search (str): Поисковая фраза.
            model_name (Optional[str]): Название модели.
            element_type (TypeEnum): Тип элемента.

        Returns:
            dict[str, SemanticObjects]: Словарь, где ключи - названия моделей, а значения - списки элементов.
        """
        models = await self._get_models_by_tenant_name(tenant_name, model_name)
        model_names_by_ids = {model.id: model.name for model in models}
        model_ids = list(model_names_by_ids.keys())

        dimensions = (
            (await self._get_dimensions_by_model_ids(model_ids=model_ids, search=search))
            if not element_type or element_type == SemanticObjectsTypeEnum.DIMENSION
            else []
        )

        data_storages = (
            (await self._get_data_storages_by_model_ids(model_ids=model_ids, search=search))
            if not element_type or element_type == SemanticObjectsTypeEnum.DATA_STORAGE
            else []
        )
        measures = (
            (
                await self._get_find_measures_by_model_ids(
                    model_ids=model_ids,
                    search=search,
                )
            )
            if not element_type or element_type == SemanticObjectsTypeEnum.MEASURE
            else []
        )

        composites = (
            (await self._get_find_composites_by_model_ids(model_ids=model_ids, search=search))
            if not element_type or element_type == SemanticObjectsTypeEnum.COMPOSITE
            else []
        )

        result = {v: SemanticObjects() for k, v in model_names_by_ids.items()}

        for dimension in dimensions:
            result[dimension.model_name].dimensions.append(dimension.name)

        for data_storage in data_storages:
            result[data_storage.model_name].data_storages.append(data_storage.name)

        for measure in measures:
            result[measure.model_name].measures.append(measure.name)

        for composite in composites:
            result[composite.model_name].composites.append(composite.name)

        return result

    async def _get_dimensions_by_model_ids(
        self, model_ids: list[int], search: Optional[str]
    ) -> Sequence[BaseModelData]:
        """
        Поиск dimensions по названию,лейблам и id модели
        Args:
            model_ids (list[int]): Список идентификаторов моделей
            search (Optional[str]): Строка, по которой ищем объекты dimensions
        Returns:
            Sequence[BaseModelData]: Последовательность типа данных BaseModelData, которая содержит в себе:
            1. Идентификатор объекта семантики
            2. Имя объекта семантики
            3. Имя модели, к которой относится объект семантики
        """
        query = (
            select(Dimension.id, Dimension.name)
            .select_from(Dimension)
            .join(
                DimensionModelRelation,
                onclause=and_(
                    DimensionModelRelation.dimension_id == Dimension.id,
                    DimensionModelRelation.model_id.in_(model_ids),
                ),
            )
            .join(Model, Model.id == DimensionModelRelation.model_id)
            .join(DimensionLabel, DimensionLabel.dimension_id == Dimension.id, isouter=True)
            .where(
                or_(
                    Dimension.name.like(f"%{search}%"),
                    DimensionLabel.text.like(f"%{search}%"),
                )
            )
            .add_columns(
                Model.name.label("model_name"),
            )
        )
        raw_tuples = await self.session.execute(query)
        return self._map_raw_tuples_to_base_model_data(raw_tuples=raw_tuples.unique().fetchall())

    async def _get_data_storages_by_model_ids(
        self, model_ids: list[int], search: Optional[str]
    ) -> Sequence[BaseModelData]:
        """
        Поиск data_storages по названию,лейблам и id модели
        Args:
            model_ids (list[int]): Список идентификаторов моделей
            search (Optional[str]): Строка, по которой ищем объекты dimensions
        Returns:
            Sequence[BaseModelData]: Последовательность типа данных BaseModelData, которая содержит в себе:
            1. Идентификатор объекта семантики
            2. Имя объекта семантики
            3. Имя модели, к которой относится объект семантики
        """
        raw_tuples = await self.session.execute(
            select(DataStorage.id, DataStorage.name)
            .select_from(DataStorage)
            .join(
                DataStorageModelRelation,
                onclause=and_(
                    DataStorageModelRelation.data_storage_id == DataStorage.id,
                    DataStorageModelRelation.model_id.in_(model_ids),
                ),
            )
            .join(
                Model,
                Model.id == DataStorageModelRelation.model_id,
            )
            .join(DataStorageLabel, onclause=DataStorageLabel.data_storage_id == DataStorage.id, isouter=True)
            .where(
                or_(
                    DataStorage.name.like(f"%{search}%"),
                    DataStorageLabel.text.like(f"%{search}%"),
                )
            )
            .add_columns(
                Model.name.label("model_name"),
            )
        )
        return self._map_raw_tuples_to_base_model_data(raw_tuples=raw_tuples.unique().fetchall())

    async def _get_find_measures_by_model_ids(
        self, model_ids: list[int], search: Optional[str]
    ) -> Sequence[BaseModelData]:
        """Поиск measures по названию,лейблам и id модели
        Args:
            model_ids (list[int]): Список идентификаторов моделей
            search (Optional[str]): Строка, по которой ищем объекты dimensions
        Returns:
            Sequence[BaseModelData]: Последовательность типа данных BaseModelData, которая содержит в себе:
            1. Идентификатор объекта семантики
            2. Имя объекта семантики
            3. Имя модели, к которой относится объект семантики
        """

        raw_tuples = await self.session.execute(
            select(Measure.id, Measure.name)
            .select_from(Measure)
            .join(
                MeasureModelRelation,
                onclause=and_(
                    MeasureModelRelation.measure_id == Measure.id, MeasureModelRelation.model_id.in_(model_ids)
                ),
            )
            .join(Model, Model.id == MeasureModelRelation.model_id)
            .join(MeasureLabel, onclause=MeasureLabel.measure_id == Measure.id, isouter=True)
            .where(
                or_(
                    Measure.name.like(f"%{search}%"),
                    MeasureLabel.text.like(f"%{search}%"),
                )
            )
            .add_columns(
                Model.name.label("model_name"),
            )
        )
        return self._map_raw_tuples_to_base_model_data(raw_tuples=raw_tuples.unique().fetchall())

    async def _get_find_composites_by_model_ids(
        self, model_ids: list[int], search: Optional[str]
    ) -> Sequence[BaseModelData]:
        """
        Поиск composites по названию,лейблам и id модели
        Args:
            model_ids (list[int]): Список идентификаторов моделей
            search (Optional[str]): Строка, по которой ищем объекты dimensions
        Returns:
            Sequence[BaseModelData]: Последовательность типа данных BaseModelData, которая содержит в себе:
            1. Идентификатор объекта семантики
            2. Имя объекта семантики
            3. Имя модели, к которой относится объект семантики
        """

        raw_tuples = await self.session.execute(
            select(Composite.id, Composite.name)
            .select_from(Composite)
            .join(
                CompositeModelRelation,
                onclause=and_(
                    CompositeModelRelation.composite_id == Composite.id, CompositeModelRelation.model_id.in_(model_ids)
                ),
            )
            .join(Model, onclause=Model.id == CompositeModelRelation.model_id)
            .join(CompositeLabel, onclause=CompositeLabel.composite_id == Composite.id, isouter=True)
            .where(
                or_(
                    Composite.name.like(f"%{search}%"),
                    CompositeLabel.text.like(f"%{search}%"),
                )
            )
            .add_columns(
                Model.name.label("model_name"),
            )
        )
        return self._map_raw_tuples_to_base_model_data(raw_tuples=raw_tuples.unique().fetchall())

    @staticmethod
    def _map_raw_tuples_to_base_model_data(raw_tuples: Sequence[Row[tuple]]) -> list[BaseModelData]:
        """
        маппринг кортежей в BaseModelData после выборки из  базы
        Args:
            raw_tuples (Sequence[Row[tuple]]): Последовательность кортейжей выборки из бд
        Returns:
            list[BaseModelData]: Последовательность типа данных BaseModelData, которая содержит в себе:
            1. Идентификатор объекта семантики
            2. Имя объекта семантики
            3. Имя модели, к которой относится объект семантики
        """
        result = []
        for raw_tuple in raw_tuples:
            result.append(
                BaseModelData(
                    id=raw_tuple[0],
                    name=raw_tuple[1],
                    model_name=raw_tuple[2],
                )
            )
        return result

    async def _get_models_by_tenant_name(self, tenant_name: str, model_name: Optional[str] = None) -> Sequence[Model]:
        """
        Получить модели по имени тенанта.
        Args:
            tenant_name (str): название тенанта
            model_name (Optional[str]): Имя модели (опционально)
        Returns:
            Sequence[Model]: Последовательность Моделей
        """
        query = select(Model).where(Model.tenant_id == tenant_name)
        if model_name:
            query = query.where(Model.name == model_name)

        result = await self.session.execute(query)
        return result.scalars().all()
