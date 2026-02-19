from collections import defaultdict
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.composite import Composite, CompositeModelRelation
from src.db.data_storage import DataStorage, DataStorageModelRelation
from src.db.database import Database
from src.db.dimension import Dimension, DimensionModelRelation
from src.db.measure import Measure, MeasureModelRelation
from src.db.model import Model
from src.integration.aor.model import AorKafkaObjectParent, AorType
from src.models.composite import Composite as CompositeModel, CompositeFieldRefObjectEnum
from src.models.data_storage import DataStorage as DataStorageModel
from src.models.dimension import Dimension as DimensionModel
from src.models.field import BaseFieldTypeEnum
from src.models.hierarchy import HierarchyMetaOut
from src.models.measure import DimensionValue as DimensionValueModel, Measure as MeasureModel

AOR_TYPE_ORM_MAPPING: dict[AorType, type[Dimension] | type[Measure] | type[DataStorage] | type[Composite]] = {
    AorType.DIMENSION: Dimension,
    AorType.MEASURE: Measure,
    AorType.DATASTORAGE: DataStorage,
    AorType.COMPOSITE: Composite,
}

AOR_TYPE_ORM_RELATION_MAPPING: dict[
    AorType,
    type[DimensionModelRelation]
    | type[MeasureModelRelation]
    | type[DataStorageModelRelation]
    | type[CompositeModelRelation],
] = {
    AorType.DIMENSION: DimensionModelRelation,
    AorType.MEASURE: MeasureModelRelation,
    AorType.DATASTORAGE: DataStorageModelRelation,
    AorType.COMPOSITE: CompositeModelRelation,
}

AOR_TYPE_RELATION_FIELD_MAPPING: dict[AorType, str] = {
    AorType.DIMENSION: "dimension_id",
    AorType.MEASURE: "measure_id",
    AorType.DATASTORAGE: "data_storage_id",
    AorType.COMPOSITE: "composite_id",
}


class AorRepository:
    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self.session = session

    async def set_model_to_parents(self, tenant_id: str, model_name: str, parents: list) -> None:
        query = select(Model.name, Model.version).where(Model.tenant_id == tenant_id, Model.name == model_name)  # type: ignore
        result = (await self.session.execute(query)).unique().one_or_none()
        if result is None:
            raise ValueError(f"Model with name = {model_name} not found for tenant {tenant_id}")
        parent_name, parent_version = result
        parents.append(
            AorKafkaObjectParent(
                parent_type=AorType.MODEL,
                parent_name=parent_name,
                parent_version=str(parent_version),
                parent_external_id=parent_name,
            )
        )

    async def get_owner_models_for_names(self, tenant_id: str, aor_type: AorType, names: list[str]) -> dict[str, str]:
        object_id = "id"
        stmt = (
            select(AOR_TYPE_ORM_MAPPING[aor_type].name, Model.name)
            .join(
                AOR_TYPE_ORM_RELATION_MAPPING[aor_type],
                getattr(AOR_TYPE_ORM_MAPPING[aor_type], object_id)
                == getattr(AOR_TYPE_ORM_RELATION_MAPPING[aor_type], AOR_TYPE_RELATION_FIELD_MAPPING[aor_type]),
            )
            .join(Model, AOR_TYPE_ORM_RELATION_MAPPING[aor_type].model_id == Model.id)
            .where(
                AOR_TYPE_ORM_MAPPING[aor_type].name.in_(names), AOR_TYPE_ORM_MAPPING[aor_type].tenant_id == tenant_id
            )
            .where(AOR_TYPE_ORM_RELATION_MAPPING[aor_type].is_owner)
        )
        results = (await self.session.execute(stmt)).all()

        result_dict: dict[str, str] = dict(results)  # type: ignore
        for name in names:
            if name not in result_dict:
                raise ValueError(f"{aor_type}  with name = {name} not found or owner not found")
        return result_dict

    async def _get_parents_by_names_and_type(
        self,
        tenant_id: str,
        object_names: list[str],
        aor_type: AorType,
    ) -> list[AorKafkaObjectParent]:
        parents = []
        orm_type = AOR_TYPE_ORM_MAPPING[aor_type]
        get_dimensions_info_query = select(orm_type.name, orm_type.version).where(  # type: ignore
            orm_type.name.in_(object_names), orm_type.tenant_id == tenant_id
        )
        dimensions_info = (await self.session.execute(get_dimensions_info_query)).tuples()
        for dimension_info in dimensions_info:
            dimension_name = dimension_info[0]
            dimension_version: int = dimension_info[1]
            parents.append(
                AorKafkaObjectParent(
                    parent_type=aor_type,
                    parent_name=dimension_name,
                    parent_version=str(dimension_version),
                    parent_external_id=dimension_name,
                )
            )
        return parents

    async def get_model_parents_by_names(
        self, tenant_id: str, model_names: list[str]
    ) -> dict[str, list[AorKafkaObjectParent]]:
        """Получить родителей модели"""
        query = (
            select(Model.name, Database.name, Database.version)  # type: ignore
            .select_from(Model)
            .join(Database, Database.id == Model.database_id)
            .where(Model.name.in_(model_names), Model.tenant_id == tenant_id)
        )
        result = (await self.session.execute(query)).tuples()
        parents = defaultdict(list)
        for parent in result:
            parents[parent[0]].append(
                AorKafkaObjectParent(
                    parent_type=AorType.DATABASE,
                    parent_name=parent[1],
                    parent_version=str(parent[2]),
                    parent_external_id=parent[1],
                )
            )
        return parents

    async def get_measure_aor_space_by_names(self, tenant_id: str, measure_names: list[str]) -> dict[str, UUID]:
        query = (
            select(Measure.name, Model.aor_space_id, Model.name)
            .select_from(MeasureModelRelation)
            .join(Model, Model.id == MeasureModelRelation.model_id)
            .join(Measure, Measure.id == MeasureModelRelation.measure_id)
            .where(Measure.name.in_(measure_names), Measure.tenant_id == tenant_id, MeasureModelRelation.is_owner)
        )
        models_tuples = (await self.session.execute(query)).tuples()
        measure_aor_id_mapping: dict[str, UUID] = {}
        for model_tuple in models_tuples:
            measure_name = model_tuple[0]
            aor_space_id = model_tuple[1]
            model_name = model_tuple[2]
            if not aor_space_id:
                raise ValueError(
                    f"AOR space not found for model {tenant_id}.{model_name} and measure {tenant_id}.{measure_name}"
                )
            measure_aor_id_mapping[measure_name] = aor_space_id

        if not measure_aor_id_mapping.get(measure_name):
            raise ValueError(f"Owner model not found for measure: {tenant_id}.{measure_name}")
        return measure_aor_id_mapping

    async def get_measure_parents_by_schema(
        self,
        tenant_id: str,
        measure_model: MeasureModel,
    ) -> list[AorKafkaObjectParent]:
        dimensions_names = set()
        owner_model_name: str | None = (
            await self.get_owner_models_for_names(tenant_id, AorType.MEASURE, [measure_model.name])
        ).get(measure_model.name)
        if not owner_model_name:
            raise ValueError(f"Measure {measure_model.name} have not owner model")
        for dimensions_filter in measure_model.filter:
            dimensions_names.add(dimensions_filter.dimension_name)
        if measure_model.unit_of_measure and isinstance(measure_model.unit_of_measure, str):
            dimensions_names.add(measure_model.unit_of_measure)
        elif measure_model.unit_of_measure and isinstance(measure_model.unit_of_measure, DimensionValueModel):
            dimensions_names.add(measure_model.unit_of_measure.dimension_name)
        parents: list[AorKafkaObjectParent] = []
        await self.set_model_to_parents(tenant_id, owner_model_name, parents)
        parents.extend(await self._get_parents_by_names_and_type(tenant_id, list(dimensions_names), AorType.DIMENSION))
        return parents

    async def get_hierarchy_parents_by_schema(
        self,
        tenant_id: str,
        hierarchy_model: HierarchyMetaOut,
    ) -> list[AorKafkaObjectParent]:
        dimension_names = set()
        if hierarchy_model.base_dimension:
            dimension_names.add(hierarchy_model.base_dimension)
        for dimension_name in hierarchy_model.additional_dimensions:
            dimension_names.add(dimension_name)
        parents = await self._get_parents_by_names_and_type(tenant_id, list(dimension_names), AorType.DIMENSION)
        return parents

    async def get_dimension_aor_space_by_names(self, tenant_id: str, dimensions_names: list[str]) -> dict[str, UUID]:
        query = (
            select(Dimension.name, Model.aor_space_id, Model.name)
            .select_from(DimensionModelRelation)
            .join(Model, Model.id == DimensionModelRelation.model_id)
            .join(Dimension, Dimension.id == DimensionModelRelation.dimension_id)
            .where(
                Dimension.name.in_(dimensions_names), Dimension.tenant_id == tenant_id, DimensionModelRelation.is_owner
            )
        )
        models_tuples = (await self.session.execute(query)).tuples()
        dimension_aor_id_mapping: dict[str, UUID] = {}
        for model_tuple in models_tuples:
            dimension_name = model_tuple[0]
            aor_space_id = model_tuple[1]
            model_name = model_tuple[2]
            if not aor_space_id:
                raise ValueError(
                    f"AOR space not found for model {tenant_id}.{model_name} and dimension {tenant_id}.{dimension_name}"
                )
            dimension_aor_id_mapping[dimension_name] = aor_space_id
        for dimension_name in dimensions_names:
            if not dimension_aor_id_mapping.get(dimension_name):
                raise ValueError(f"Owner model not found for dimension: {tenant_id}.{dimension_name}")
        return dimension_aor_id_mapping

    async def get_dimension_parents_by_schema(
        self,
        tenant_id: str,
        dimension_model: DimensionModel,
    ) -> list[AorKafkaObjectParent]:
        dimension_names = set()
        measure_names = set()
        owner_model_name: str | None = (
            await self.get_owner_models_for_names(tenant_id, AorType.DIMENSION, [dimension_model.name])
        ).get(dimension_model.name)
        if not owner_model_name:
            raise ValueError(f"Dimensions {dimension_model.name} have not owner model")
        if dimension_model.dimension_name:
            dimension_names.add(dimension_model.dimension_name)
        else:
            for attribute in dimension_model.attributes:
                if attribute.ref_type.ref_object_type == BaseFieldTypeEnum.DIMENSION and isinstance(
                    attribute.ref_type.ref_object, str
                ):
                    dimension_names.add(attribute.ref_type.ref_object)
                elif attribute.ref_type.ref_object_type == BaseFieldTypeEnum.MEASURE and isinstance(
                    attribute.ref_type.ref_object, str
                ):
                    measure_names.add(attribute.ref_type.ref_object)
        parents: list[AorKafkaObjectParent] = []
        await self.set_model_to_parents(tenant_id, owner_model_name, parents)
        if dimension_names:
            parents.extend(
                await self._get_parents_by_names_and_type(tenant_id, list(dimension_names), AorType.DIMENSION)
            )
        if measure_names:
            parents.extend(await self._get_parents_by_names_and_type(tenant_id, list(measure_names), AorType.MEASURE))
        return parents

    async def get_datastorage_parents_by_schema(
        self,
        tenant_id: str,
        datastorage_model: DataStorageModel,
    ) -> list[AorKafkaObjectParent]:
        dimension_names = set()
        measure_names = set()
        owner_model_name: str | None = (
            await self.get_owner_models_for_names(tenant_id, AorType.DATASTORAGE, [datastorage_model.name])
        ).get(datastorage_model.name)
        if not owner_model_name:
            raise ValueError(f"Datastorage {datastorage_model.name} have not owner model")
        for field in datastorage_model.fields:
            if field.ref_type.ref_object_type == BaseFieldTypeEnum.DIMENSION and isinstance(
                field.ref_type.ref_object, str
            ):
                dimension_names.add(field.ref_type.ref_object)
            elif field.ref_type.ref_object_type == BaseFieldTypeEnum.MEASURE and isinstance(
                field.ref_type.ref_object, str
            ):
                measure_names.add(field.ref_type.ref_object)
        parents: list[AorKafkaObjectParent] = []
        await self.set_model_to_parents(tenant_id, owner_model_name, parents)
        if dimension_names:
            parents.extend(
                await self._get_parents_by_names_and_type(tenant_id, list(dimension_names), AorType.DIMENSION)
            )
        if measure_names:
            parents.extend(await self._get_parents_by_names_and_type(tenant_id, list(measure_names), AorType.MEASURE))
        return parents

    async def get_datastorage_parents_by_names(
        self, tenant_id: str, datastorage_names: list[str]
    ) -> list[AorKafkaObjectParent]:
        """Возвращает родителей AOR для VIEW как список DATA_STORAGE по именам хранилищ."""
        if not datastorage_names:
            return []
        return await self._get_parents_by_names_and_type(tenant_id, datastorage_names, AorType.DATASTORAGE)

    async def get_datastorage_aor_space_by_names(self, tenant_id: str, datastorage_names: list[str]) -> dict[str, UUID]:
        query = (
            select(DataStorage.name, Model.aor_space_id, Model.name)
            .select_from(DataStorageModelRelation)
            .join(Model, Model.id == DataStorageModelRelation.model_id)
            .join(DataStorage, DataStorage.id == DataStorageModelRelation.data_storage_id)
            .where(
                DataStorage.name.in_(datastorage_names),
                DataStorage.tenant_id == tenant_id,
                DataStorageModelRelation.is_owner,
            )
        )
        models_tuples = (await self.session.execute(query)).tuples()
        datastorage_aor_id_mapping: dict[str, UUID] = {}
        for model_tuple in models_tuples:
            datastorage_name = model_tuple[0]
            aor_space_id = model_tuple[1]
            model_name = model_tuple[2]
            if not aor_space_id:
                raise ValueError(
                    f"AOR space not found for model {tenant_id}.{model_name} and datastorage {tenant_id}.{datastorage_name}"
                )
            datastorage_aor_id_mapping[datastorage_name] = aor_space_id
        for datastorage_name in datastorage_names:
            if not datastorage_aor_id_mapping.get(datastorage_name):
                raise ValueError(f"Owner model not found for datastorage: {tenant_id}.{datastorage_name}")
        return datastorage_aor_id_mapping

    async def get_composite_parents_by_schema(
        self,
        tenant_id: str,
        composite_model: CompositeModel,
    ) -> list[AorKafkaObjectParent]:
        dimension_names = set()
        measure_names = set()
        composite_names = set()
        datastorage_names = set()
        owner_model_name: str | None = (
            await self.get_owner_models_for_names(tenant_id, AorType.COMPOSITE, [composite_model.name])
        ).get(composite_model.name)
        if not owner_model_name:
            raise ValueError(f"Composite {composite_model.name} have not owner model")
        for field in composite_model.fields:
            if field.ref_type.ref_object_type == BaseFieldTypeEnum.DIMENSION and isinstance(
                field.ref_type.ref_object, str
            ):
                dimension_names.add(field.ref_type.ref_object)
            elif field.ref_type.ref_object_type == BaseFieldTypeEnum.MEASURE and isinstance(
                field.ref_type.ref_object, str
            ):
                measure_names.add(field.ref_type.ref_object)
        for datasource in composite_model.datasources:
            if datasource.type == CompositeFieldRefObjectEnum.DATASTORAGE:
                datastorage_names.add(datasource.name)
            elif datasource.type == CompositeFieldRefObjectEnum.COMPOSITE:
                composite_names.add(datasource.name)
        parents: list[AorKafkaObjectParent] = []
        await self.set_model_to_parents(tenant_id, owner_model_name, parents)
        if dimension_names:
            parents.extend(
                await self._get_parents_by_names_and_type(tenant_id, list(dimension_names), AorType.DIMENSION)
            )
        if measure_names:
            parents.extend(await self._get_parents_by_names_and_type(tenant_id, list(measure_names), AorType.MEASURE))
        if composite_names:
            parents.extend(
                await self._get_parents_by_names_and_type(tenant_id, list(composite_names), AorType.COMPOSITE)
            )
        if datastorage_names:
            parents.extend(
                await self._get_parents_by_names_and_type(tenant_id, list(datastorage_names), AorType.DATASTORAGE)
            )
        return parents

    async def get_composite_aor_space_by_names(self, tenant_id: str, composite_names: list[str]) -> dict[str, UUID]:
        query = (
            select(Composite.name, Model.aor_space_id, Model.name)
            .select_from(CompositeModelRelation)
            .join(Model, Model.id == CompositeModelRelation.model_id)
            .join(Composite, Composite.id == CompositeModelRelation.composite_id)
            .where(
                Composite.name.in_(composite_names),
                Composite.tenant_id == tenant_id,
                CompositeModelRelation.is_owner,
            )
        )
        models_tuples = (await self.session.execute(query)).tuples()
        composite_aor_id_mapping: dict[str, UUID] = {}
        for model_tuple in models_tuples:
            composite_name = model_tuple[0]
            aor_space_id = model_tuple[1]
            model_name = model_tuple[2]
            if not aor_space_id:
                raise ValueError(
                    f"AOR space not found for model {tenant_id}.{model_name} and composite {tenant_id}.{composite_name}"
                )
            composite_aor_id_mapping[composite_name] = aor_space_id
        for composite_name in composite_names:
            if not composite_aor_id_mapping.get(composite_name):
                raise ValueError(f"Owner model not found for composite: {tenant_id}.{composite_name}")
        return composite_aor_id_mapping

    @classmethod
    def get_by_session(cls, session: AsyncSession) -> "AorRepository":
        """
        Получить экземпляр AorRepository с использованием сессии.
        Args:
            session (AsyncSession): Сессия для работы с базой данных.
        Returns:
            AorRepository: Экземпляр AorRepository.
        """
        return cls(session)
