"""
Сервис иерархий.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Optional
from uuid import UUID

from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy.exc import NoResultFound
from starlette_context import context

from src.config import settings
from src.db import (
    DataStorage,
    Dimension,
    HierarchyBaseDimension,
    HierarchyLabel,
    HierarchyMeta,
    HierarchyModelRelation,
    Model,
)
from src.integration.aor import ClientAOR
from src.integration.aor.model import AorType, CreateAorCommand, JsonData
from src.models.any_field import AnyFieldTypeEnum
from src.models.data_storage import DataStorageCreateRequest, DataStorageEnum, DataStorageFieldRequest
from src.models.dimension import DefaultDimensionEnum
from src.models.hierarchy import (
    HierarchyCopyResponse,
    HierarchyCopyStatus,
    HierarchyCreateRequest,
    HierarchyEditRequest,
    HierarchyMetaOut,
)
from src.repository.aor import AorRepository
from src.repository.cache import CacheRepository
from src.repository.hierarchy import HierarchyRepository
from src.repository.history.hierarchy import HierarchyHistoryRepository
from src.repository.utils import add_missing_labels, convert_labels_list_to_orm
from src.service.data_storage import DataStorageService
from src.service.database import DatabaseService
from src.service.dimension import DimensionService
from src.service.model import ModelService
from src.service.pv_hierarchy import HierarchyPvdService
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry
from src.utils.validators import get_index_or_default

logger = EPMPYLogger(__name__)


@dataclass
class DimensionFieldData:
    type: AnyFieldTypeEnum
    name: str
    dimension_name: str
    name_ru_short: str | None = None
    name_ru_long: str | None = None
    is_key: bool = False
    precision: int = 3
    is_tech_fields: bool = False


HIERARCHY_DIMENSION_FIELDS = {
    "hierarchy_name": DimensionFieldData(
        precision=32, type=AnyFieldTypeEnum.STRING, name="hierarchy_name", dimension_name="hierarchy_name"
    ),
    "hierarchy_version_id": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING, name="hierarchy_version_id", is_key=True, dimension_name="hierarchy_version_id"
    ),
    "version_code": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING, name="version_code", dimension_name="version_code"
    ),
    "language": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING,
        precision=5,
        name="language",
        dimension_name="language_tag",
        is_key=True,
    ),
    "txtshort": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING, precision=20, name="txtshort", dimension_name="txtshort"
    ),
    "txtmedium": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING, precision=40, name="txtmedium", dimension_name="txtmedium"
    ),
    "txtlong": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING, precision=1333, name="txtlong", dimension_name="txtlong"
    ),
    "dateto": DimensionFieldData(
        type=AnyFieldTypeEnum.DATE,
        name="dateto",
        dimension_name="dateto",
        precision=10,
        is_key=True,
    ),
    "datefrom": DimensionFieldData(
        type=AnyFieldTypeEnum.DATE,
        name="datefrom",
        dimension_name="datefrom",
        precision=10,
    ),
    "node_id": DimensionFieldData(
        type=AnyFieldTypeEnum.INTEGER,
        name="node_id",
        dimension_name="node_id",
        is_key=True,
        precision=10,
    ),
    "node_type": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING,
        name="node_type",
        dimension_name="node_type",
    ),
    "dimension_name": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING, name="dimension_name", precision=32, dimension_name="dimension_name"
    ),
    "dimension_key": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING,
        name="dimension_key",
        dimension_name="dimension_key",
        precision=32,
    ),
    "parent_id": DimensionFieldData(
        type=AnyFieldTypeEnum.INTEGER,
        name="parent_id",
        dimension_name="parent_id",
        precision=10,
    ),
    "child_id": DimensionFieldData(
        type=AnyFieldTypeEnum.INTEGER,
        name="child_id",
        dimension_name="child_id",
        precision=10,
    ),
    "next_id": DimensionFieldData(
        type=AnyFieldTypeEnum.INTEGER,
        name="next_id",
        dimension_name="next_id",
        precision=10,
    ),
    "nexts_string": DimensionFieldData(
        type=AnyFieldTypeEnum.STRING,
        name="nexts_string",
        dimension_name="nexts_string",
        precision=1333,
    ),
    "node_level": DimensionFieldData(
        type=AnyFieldTypeEnum.INTEGER,
        name="node_level",
        dimension_name="node_level",
        precision=10,
    ),
    "parent_array": DimensionFieldData(
        type=AnyFieldTypeEnum.ARRAY_INTEGER, name="parent_array", precision=1024, dimension_name="parent_array"
    ),
    "child_array": DimensionFieldData(
        type=AnyFieldTypeEnum.ARRAY_INTEGER, name="child_array", precision=1024, dimension_name="child_array"
    ),
    "timestamp": DimensionFieldData(
        type=AnyFieldTypeEnum.TIMESTAMP,
        name="timestamp",
        dimension_name="timestamp",
        precision=26,
        is_tech_fields=True,
    ),
    "deleted": DimensionFieldData(
        type=AnyFieldTypeEnum.BOOLEAN,
        name="deleted",
        dimension_name="deleted",
        is_tech_fields=True,
    ),
    DefaultDimensionEnum.HIERARCHY_TEXTNODES: DimensionFieldData(
        type=AnyFieldTypeEnum.STRING,
        name=DefaultDimensionEnum.HIERARCHY_TEXTNODES,
        dimension_name=DefaultDimensionEnum.HIERARCHY_TEXTNODES,
        name_ru_short="Текст. узлы иерархии",
        name_ru_long="Текст. узлы иерархии",
        precision=32,
    ),
}


class HierarchyService:
    def __init__(
        self,
        hierarchy_repo: HierarchyRepository,
        dimension_service: DimensionService,
        data_storage_service: DataStorageService,
        database_service: DatabaseService,
        model_service: ModelService,
        aor_client: ClientAOR,
        hierarchy_history_repo: HierarchyHistoryRepository,
        aor_repository: AorRepository,
        pvd_service: Optional[HierarchyPvdService] = None,
    ) -> None:
        self.aor_repository = aor_repository
        self.hierarchy_repo: HierarchyRepository = hierarchy_repo
        self.dimension_service: DimensionService = dimension_service
        self.data_storage_service: DataStorageService = data_storage_service
        self.database_service: DatabaseService = database_service
        self.model_service: ModelService = model_service
        self.aor_client = aor_client
        self.hierarchy_history_repo = hierarchy_history_repo
        self.pvd_service = pvd_service

    def _is_pvd_meta_sync_enabled(self) -> bool:
        return bool(settings.ENABLE_PV_HIERARCHIES_META_SYNC and self.pvd_service is not None)

    async def _enrich_hierarchy_orm_with_dimension_data(self, hierarchy: HierarchyMeta) -> HierarchyMeta:
        """
        Обогащение иерархии дополнительными измерениями из базы данных.

        Args:
            hierarchy (HierarchyMeta): Иерархия, которую нужно обогатить информацией о базовых и дополнительных измерениях

        Returns:
            HierarchyMeta: Объект иерархии с добавленными базовым и дополнительным измерением
        """
        dimensions = await self.hierarchy_repo.get_base_dimension_names_by_hierarchy_id(hierarchy.id)
        hierarchy.base_dimension = next(dimension_tuple[0] for dimension_tuple in dimensions if dimension_tuple[1])  # type: ignore[attr-defined]
        hierarchy.additional_dimensions = [  # type: ignore[attr-defined]
            dimension_tuple[0] for dimension_tuple in dimensions if not dimension_tuple[1]
        ]
        return hierarchy

    @retry(RetryConfig())
    async def get_hierarchy_by_hierarchy_name(self, name: str, model_name: str) -> list[HierarchyMetaOut]:
        """
        Получение списка иерархий по заданному имени иерархии и имени модели.

        Args:
            name (str): Название искомой иерархии
            model_name (str): Название модели, в рамках которой ищем иерархию

        Returns:
            List[HierarchyMetaOut]: Список найденных иерархий, соответствующих указанным параметрам
        """
        hierarchies = await self.hierarchy_repo.get_by_name(name=name, model_name=model_name)
        result = []
        for hierarchy in hierarchies:
            out_model = HierarchyMetaOut.model_validate(await self._enrich_hierarchy_orm_with_dimension_data(hierarchy))
            result.append(out_model)

        return result

    def get_dimension_name_and_hierarchy_name_by_name(self, name: str, sep: str = "__") -> tuple[str, str]:
        names = name.split(sep)
        if len(names) != 2:
            raise ValueError("""The hierarchy name must match the following pattern: "{dimension}__{hierarchy}".""")
        dimension_name = names[0]
        hierarchy_name = names[1]
        return dimension_name, hierarchy_name

    async def get_hierarchy_by_hierarchy_name_and_sep(
        self, tenant_id: str, name: str, model_name: Optional[str] = None, sep: str = "__"
    ) -> HierarchyMetaOut:
        """Получить иерархию по имени."""
        dimension_name, hierarchy_name = self.get_dimension_name_and_hierarchy_name_by_name(name)
        hierarchy_models = await self.get_multi(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=[dimension_name],
            hierarchy_names=[hierarchy_name],
        )
        return hierarchy_models[0]

    async def get_hierarchy_by_hierarchy_name_and_sep_or_null(
        self, tenant_id: str, name: str, model_name: Optional[str] = None, sep: str = "__"
    ) -> Optional[HierarchyMetaOut]:
        """Получить иерархию по имени."""
        try:
            return await self.get_hierarchy_by_hierarchy_name_and_sep(
                tenant_id,
                name,
                model_name,
                sep,
            )
        except NoResultFound:
            return None

    @retry(RetryConfig())
    async def get_multi(
        self,
        tenant_id: str | None,
        model_name: str | None,
        dimension_names: list[str] | None,
        hierarchy_names: list[str],
    ) -> list[HierarchyMetaOut]:
        """
        Получает несколько иерархий одновременно по спискам названий измерений и иерархий.

        Args:
            model_name (str): Имя модели, внутри которой производится поиск иерархий.
            dimension_names (List[str]): Список имен измерений, используемых для фильтрации результатов.
            hierarchy_names (List[str]): Список имен иерархий, которые нужно получить.

        Returns:
            List[HierarchyMetaOut]: Список объектов иерархий, удовлетворяющих условиям поиска.
        """
        orm_result = await self.hierarchy_repo.get_list(
            tenant_id=tenant_id,
            model_name=model_name,
            dimension_names=dimension_names,
            hierarchy_names=hierarchy_names,
        )
        if not orm_result:
            raise NoResultFound
        result = []
        for hierarchy in orm_result:
            enriched_hierarchy = await self._enrich_hierarchy_orm_with_dimension_data(hierarchy)
            out_model = HierarchyMetaOut.model_validate(enriched_hierarchy)
            result.append(out_model)

        return result

    async def _create_hierarchy_dimension_fields(
        self,
        model: Model,
    ) -> None:
        """
        Внутренний метод для создания полей измерения иерархии на основе указанной модели.

        Args:
            model (Model): Модель, на основании которой создаются поля измерения иерархии.

        Returns:
            None: Этот метод ничего не возвращает, он производит изменения непосредственно в структуре модели.
        """
        hierarchy_dimension_field_data = {}
        for dimension_field_data in HIERARCHY_DIMENSION_FIELDS.values():
            hierarchy_dimension_field_data[dimension_field_data.name] = {
                "precision": dimension_field_data.precision,
                "type": dimension_field_data.type,
                "name_ru_short": dimension_field_data.name_ru_short,
                "name_ru_long": dimension_field_data.name_ru_long,
            }

        await self.data_storage_service.data_repository.create_fields_dimensions_if_not_exists(
            model=model, fields_to_create=hierarchy_dimension_field_data, is_virtual=True
        )

    def _get_datastorage_versions_fields(self, model_name: str | None) -> list[dict]:
        """
        Формирует список полей, необходимых для описания версий хранилищ данных.
        Args:
        Returns:
            list[dict]: Список словарей, каждый из которых содержит набор полей версии хранилища данных.
        """
        dimension_names = [
            "hierarchy_name",
            "hierarchy_version_id",
            "version_code",
            "datefrom",
            "dateto",
            "timestamp",
            "deleted",
        ]
        return [
            self.data_storage_service.data_repository.create_dimension_data_storage_field_dict(
                dimension_name=HIERARCHY_DIMENSION_FIELDS[name].dimension_name,
                ds_field_name=HIERARCHY_DIMENSION_FIELDS[name].name,
                is_key=HIERARCHY_DIMENSION_FIELDS[name].is_key,
                is_tech_field=HIERARCHY_DIMENSION_FIELDS[name].is_tech_fields,
            )
            for name in dimension_names
        ]

    def _get_datastorage_textversions_fields(self) -> list[dict]:
        """
        Формирование списка полей для текстовых версий хранилищ данных.

        Returns:
            list[dict]: Список словарей, содержащих необходимые поля для каждой текстовой версии хранилища данных.
        """
        dimension_names = ["hierarchy_version_id", "language", "txtshort", "txtmedium", "txtlong"]
        return [
            self.data_storage_service.data_repository.create_dimension_data_storage_field_dict(
                dimension_name=HIERARCHY_DIMENSION_FIELDS[name].dimension_name,
                ds_field_name=HIERARCHY_DIMENSION_FIELDS[name].name,
                is_key=HIERARCHY_DIMENSION_FIELDS[name].is_key,
            )
            for name in dimension_names
        ]

    def _get_datastorage_nodes_fields(self) -> list[dict]:
        """
        Формирование списка полей для узлов хранилища данных.

        Returns:
            list[dict]: Список словарей, каждый из которых содержит набор полей узла хранилища данных.
        """
        dimension_names = [
            "hierarchy_version_id",
            "node_id",
            "dimension_name",
            "node_type",
            "dimension_key",
            "parent_id",
            "parent_array",
            "child_id",
            "child_array",
            "next_id",
            "nexts_string",
            "node_level",
            "datefrom",
            "dateto",
        ]
        return [
            self.data_storage_service.data_repository.create_dimension_data_storage_field_dict(
                dimension_name=HIERARCHY_DIMENSION_FIELDS[name].dimension_name,
                ds_field_name=HIERARCHY_DIMENSION_FIELDS[name].name,
                is_key=HIERARCHY_DIMENSION_FIELDS[name].is_key,
            )
            for name in dimension_names
        ]

    def _get_datastorage_textnodes_fields(self) -> list[dict]:
        """
        Формирование списка полей для текстовых узлов хранилища данных.

        Returns:
            list[dict]: Список словарей, каждый из которых содержит набор полей одного текстового узла хранилища данных.
        """
        dimension_names = ["hierarchy_version_id", "node_id", "language", "txtshort", "txtmedium", "txtlong"]
        return [
            self.data_storage_service.data_repository.create_dimension_data_storage_field_dict(
                dimension_name=HIERARCHY_DIMENSION_FIELDS[name].dimension_name,
                ds_field_name=HIERARCHY_DIMENSION_FIELDS[name].name,
                is_key=HIERARCHY_DIMENSION_FIELDS[name].is_key,
            )
            for name in dimension_names
        ]

    async def _create_hierarchy_data_storage(
        self, ds_name: str, model: Model, ds_type: DataStorageEnum, fields_data_to_create: list[dict]
    ) -> DataStorage:
        """
        Создание хранилища данных для иерархии.

        Args:
            ds_name (str): Имя создаваемого хранилища данных.
            model (Model): Экземпляр модели, к которой привязывается хранилище данных.
            ds_type (DataStorageEnum): Тип создаваемого хранилища данных (например, версия, узлы и др.).
            fields_data_to_create (list[dict]): Список словарей, содержащие данные полей, которые нужно создать в хранилище.

        Returns:
            DataStorage: Объект созданного хранилища данных.
        """
        return await self.data_storage_service.create_data_storage_by_schema_if_not_exists(
            tenant_id=model.tenant_id,
            model_name=model.name,
            generate_physical_if_not_exists=True,
            data_storage=DataStorageCreateRequest(
                fields=[DataStorageFieldRequest(**ds_field) for ds_field in fields_data_to_create],
                name=ds_name,
                type=ds_type,
            ),
        )

    async def _create_hierarchy_data_storages_for_list_of_models(
        self, models: list[Model], dimension_name: str
    ) -> None:
        """
        Асинхронная процедура создания хранилищ данных иерархий для нескольких моделей сразу.

        Args:
            models (list[Model]): Список моделей, для которых необходимо создать хранилища данных иерархий.
            dimension_name (str): Имя измерения, используемое при создании хранилищ данных.

        Returns:
            None: Данный метод не возвращает значений, он лишь создаёт требуемые хранилища данных.
        """
        data_storages = []
        for model in models:
            data_storages.extend(
                await self._create_hierarchy_data_storages(
                    model=model,
                    dimension_name=dimension_name,
                )
            )

    async def _assign_models_to_data_storage(self, models: list[Model], data_storage: DataStorage) -> tuple[list, list]:
        """
        Назначает список моделей указанному хранилищу данных и возвращает списки успешных и неудачных назначений.

        Args:
            models (list[Model]): Список моделей, которые необходимо назначить хранилищу данных.
            data_storage (DataStorage): Хранилище данных, куда назначаются модели.

        Returns:
            tuple[list, list]: кортеж двух списков:
                - первый список содержит успешные назначения (список успешно назначенных моделей),
                - второй список содержит ошибки (список моделей, назначение которых закончилось ошибкой).
        """
        ds_models_by_names = {model.name for model in data_storage.models}
        not_ignored: list[str] = []
        ignored: list[str] = []
        for model in models:
            if model.name not in ds_models_by_names:
                not_ignored, ignored = await self.data_storage_service.data_repository.update_models_datastorage(
                    data_storage, models=[model]
                )
        return not_ignored, ignored

    async def _create_hierarchy_data_storages(self, model: Model, dimension_name: str) -> list[DataStorage]:
        """
        Создает хранилища данных для иерархии в указанном экземпляре модели.

        Args:
            model (Model): Экземпляр модели, для которой создается хранилище данных иерархии.
            dimension_name (str): Имя измерения, используемого при создании хранилищ данных.

        Returns:
            list[DataStorage]: Список созданных хранилищ данных для иерархии.
        """

        ds_hierarchy_versions = await self._create_hierarchy_data_storage(
            ds_name=f"{dimension_name}_versions",
            model=model,
            ds_type=DataStorageEnum.HIERARCHY_VERSIONS,
            fields_data_to_create=self._get_datastorage_versions_fields(model_name=model.name),
        )
        ds_hierarchy_textversions = await self._create_hierarchy_data_storage(
            ds_name=f"{dimension_name}_textversions",
            model=model,
            ds_type=DataStorageEnum.HIERARCHY_TEXTVERSIONS,
            fields_data_to_create=self._get_datastorage_textversions_fields(),
        )
        ds_hierarchy_nodes = await self._create_hierarchy_data_storage(
            ds_name=f"{dimension_name}_nodes",
            model=model,
            ds_type=DataStorageEnum.HIERARCHY_NODES,
            fields_data_to_create=self._get_datastorage_nodes_fields(),
        )
        ds_hierarchy_textnodes = await self._create_hierarchy_data_storage(
            ds_name=f"{dimension_name}_textnodes",
            model=model,
            ds_type=DataStorageEnum.HIERARCHY_TEXTNODES,
            fields_data_to_create=self._get_datastorage_textnodes_fields(),
        )
        data = [ds_hierarchy_versions, ds_hierarchy_textversions, ds_hierarchy_nodes, ds_hierarchy_textnodes]
        for ds in data:
            not_ignored, ignored = await self._assign_models_to_data_storage([model], ds)
            if not_ignored:
                await self.data_storage_service.data_repository.generate_physical(
                    tenant_id=model.tenant_id,
                    model=not_ignored[0],
                    data_storage_orm=ds,
                    generate_physical_if_not_exists=True,
                )
        return data

    async def _assign_base_dimension_to_hierarchy(self, dimension: Dimension, hierarchy: HierarchyMeta) -> None:
        """
        Присваивает базовое измерение существующей иерархии.

        Args:
            dimension (Dimension): Измерение, которое присваивается иерархии.
            hierarchy (HierarchyMeta): Иерархия, к которой добавляется базовое измерение.

        Returns:
            None: Метод не возвращает никаких значений, он изменяет состояние иерархии.
        """
        if await self.hierarchy_repo.is_hierarchy_name_has_base_dimension(
            hierarchy_name=hierarchy.name, base_dimension_id=dimension.id
        ):
            raise ValueError(f"Hierarchy {hierarchy.name} already has base dimension")

        hierarchy_base_dimension = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id, dimension_id=dimension.id, is_base=True
        )
        self.hierarchy_repo.session.add(hierarchy_base_dimension)
        await self.hierarchy_repo.session.flush()

    async def _assign_additional_dimensions_to_hierarchy(
        self, hierarchy: HierarchyMeta, additional_dimensions: list[Dimension]
    ) -> None:
        """
        Присваивает дополнительные измерения существующей иерархии.

        Args:
            hierarchy (HierarchyMeta): Иерархия, к которой добавляются дополнительные измерения.
            additional_dimensions (list[Dimension]): Список дополнительных измерений, которые нужно присвоить иерархии.

        Returns:
            None: Метод не возвращает значения, он изменяет состояние иерархии.
        """
        orm_additional_dimensions = []
        for dimension in additional_dimensions:
            orm_additional_dimensions.append(
                HierarchyBaseDimension(hierarchy_id=hierarchy.id, dimension_id=dimension.id, is_base=False)
            )

        self.hierarchy_repo.session.add_all(orm_additional_dimensions)
        await self.hierarchy_repo.session.flush()

    async def _assign_hierarchy_to_models(self, models: list[Model], hierarchy: HierarchyMeta) -> None:
        """
        Присваивает иерархию нескольким моделям.

        Args:
            models (list[Model]): Список моделей, к которым нужно присоединить иерархию.
            hierarchy (HierarchyMeta): Иерархия, которая назначается моделям.

        Returns:
            None: Метод не возвращает значения, он изменяет связи между моделями и иерархией.
        """
        existing_hierarchy_models_by_name = {model.name: model for model in hierarchy.models}

        hierarchy_models = []
        for model in models:
            if model.name in existing_hierarchy_models_by_name:
                continue
            hierarchy_models.append(HierarchyModelRelation(hierarchy_id=hierarchy.id, model_id=model.id))
        self.hierarchy_repo.session.add_all(hierarchy_models)
        await self.hierarchy_repo.session.flush()

    async def _create_hierarchy_model_relations(self, models: list[Model], hierarchy: HierarchyMeta) -> None:
        """
        Присваивает иерархию ко множеству моделей.

        Args:
            models (List[Model]): Список моделей.
            hierarchy (HierarchyMeta): Иерархия, которую нужно присвоить.
        """
        existing_hierarchy_models_by_name = {model.name: model for model in hierarchy.models}

        hierarchy_models = []
        for model in models:
            if model.name in existing_hierarchy_models_by_name:
                continue
            hierarchy_models.append(model)
            hierarchy.models.append(model)
        self.hierarchy_repo.session.add_all(hierarchy_models)

    async def assign_hierarchy_to_models(
        self, models: list[Model], hierarchy: HierarchyMeta, base_dimension_name: str
    ) -> None:
        """
        Назначает указанную иерархию в список моделей, привязывая её к базовому измерению.

        Операция включает копирование структуры иерархии и её привязку к измерению
        с именем `base_dimension_name` в каждой из переданных моделей.

        Args:
            models (list[Model]): Список моделей, в которые будет назначена иерархия.
            hierarchy (HierarchyMeta): Метаданные иерархии, которая копируется.
            base_dimension_name (str): Имя базового измерения в целевых моделях,
                                      к которому привязывается иерархия.

        Returns:
            None
        """
        await self._create_hierarchy_model_relations(models=models, hierarchy=hierarchy)
        for model in models:
            await self._create_hierarchy_dimension_fields(model=model)
            self.hierarchy_repo.session.add(model)
        await self.hierarchy_repo.session.flush()

        await self._create_hierarchy_data_storages_for_list_of_models(models=models, dimension_name=base_dimension_name)

    async def _raise_if_model_is_absent(self, model_name: str, tenant_id: str) -> None:
        """
        Проверяет существование модели по имени в указанном тенанте.

        Если модель с заданным именем не найдена, вызывает исключение HTTPException с кодом 404.

        Args:
            model_name (str): Имя модели, которую необходимо проверить.
            tenant_id (str): Идентификатор арендатора (тенанта), в котором ищется модель.

        Raises:
            HTTPException: Если модель не найдена (статус 404).

        Returns:
            None
        """
        model = await self.model_service.get_model_orm(tenant_id=tenant_id, name=model_name)
        if not model:
            raise ValueError(f"Model {model_name} not found")

    async def _get_base_dimension_or_raise(self, model_name: str, tenant_id: str, dimension_name: str) -> Dimension:
        """
        Получает базовое измерение по имени из указанной модели и тенанта.

        Если измерение не найдено, вызывает исключение HTTPException с кодом 404.

        Args:
            model_name (str): Имя модели, содержащей измерение.
            tenant_id (str): Идентификатор арендатора (тенанта).
            dimension_name (str): Имя измерения, которое необходимо получить.

        Returns:
            Dimension: Объект измерения, если найден.

        Raises:
            HTTPException: Если измерение или модель не найдены (статус 404).
        """
        base_dimension = await self.dimension_service.get_dimension_orm_model(
            tenant_id=tenant_id, model_name=model_name, name=dimension_name
        )
        if not base_dimension:
            raise ValueError(f"Base dimension {dimension_name} not found for model {model_name} in tenant {tenant_id}")
        return base_dimension

    async def create_hierarchy_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        hierarchy: HierarchyCreateRequest,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> HierarchyMetaOut:
        """
        Создает новую иерархию на основе схемы и запрашиваемых параметров.

        Args:
            tenant_id (str): Идентификатор арендатора (тенант), в рамках которого создается иерархия.
            model_name (str): Имя модели, к которой относится новая иерархия.
            dimension_name (str): Имя измерения, используемого для новой иерархии.
            hierarchy (HierarchyCreateRequest): Запрос на создание иерархии, включающий необходимую схему и метаданные.

        Returns:
            HierarchyMetaOut: Метаданные созданной иерархии после успешного завершения операции.
        """
        try:
            await self.dimension_service.data_repository.create_not_virtual_dimensions(tenant_id, [model_name])
            base_dimension = await self._get_base_dimension_or_raise(
                model_name=model_name, tenant_id=tenant_id, dimension_name=dimension_name
            )
            await self._raise_if_model_is_absent(model_name=model_name, tenant_id=tenant_id)

            base_model = get_index_or_default([model for model in base_dimension.models if model.name == model_name])
            if not base_model:
                raise ValueError(f"Model {model_name} not found")

            created_hierarchy = await self.hierarchy_repo.create_by_schema(
                hierarchy=hierarchy, base_dimension_name=base_dimension.name
            )
            self._update_hierarchy_labels([label.model_dump() for label in hierarchy.labels], created_hierarchy)
            await self._assign_base_dimension_to_hierarchy(dimension=base_dimension, hierarchy=created_hierarchy)
            await self.hierarchy_repo.session.flush()

            additional_dimensions = await self.dimension_service.data_repository.get_list_of_orm_models(
                tenant_id, model_name, hierarchy.additional_dimensions
            )
            await self._assign_additional_dimensions_to_hierarchy(created_hierarchy, list(additional_dimensions))
            await self.assign_hierarchy_to_models(
                models=base_dimension.models, hierarchy=created_hierarchy, base_dimension_name=base_dimension.name
            )

            await self.hierarchy_repo.session.flush()
            await self.hierarchy_history_repo.update_version(
                created_hierarchy, tenant_id=base_dimension.tenant_id, create=True
            )
            await self.hierarchy_repo.set_owner_model([created_hierarchy], base_model)

            if self._is_pvd_meta_sync_enabled():
                await self.pvd_service.create_hierarchy_in_pvd(  # type: ignore[union-attr]
                    tenant_id=tenant_id,
                    model_name=model_name,
                    dimension_name=dimension_name,
                    hierarchy_name=created_hierarchy.name,
                    commit=False,
                )

            await self.hierarchy_repo.session.commit()

            out_model = HierarchyMetaOut.model_validate(created_hierarchy)
            out_model.base_dimension = base_dimension.name
            out_model.additional_dimensions = [dimension.name for dimension in additional_dimensions]

            await self._clear_hierarchy_cache(
                tenant_id=tenant_id,
                dimension_name=base_dimension.name,
                hierarchy_name=created_hierarchy.name,
                models=base_dimension.models,
            )
            if send_to_aor:
                await self.create_and_send_command_to_aor_by_hierarchy(tenant_id, out_model)
            return out_model
        except Exception as exc:
            await self.hierarchy_repo.session.rollback()
            raise Exception(str(exc))

    async def _get_absent_dimension_names(self, tenant_id: str, model_name: str, hierarchy: HierarchyMeta) -> set[str]:
        """
        Возвращает множество имён измерений, отсутствующих в указанной модели,
        но используемых в переданной иерархии.

        Метод проверяет, все ли измерения, на которые ссылается иерархия, существуют в модели.
        Если какие-либо измерения отсутствуют — они включаются в результат.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            model_name (str): Имя модели, в которой проверяется наличие измерений.
            hierarchy (HierarchyMeta): Метаданные иерархии, содержащие ссылки на измерения.

        Returns:
            set[str]: Множество имён измерений, которые используются в иерархии, но отсутствуют в модели.
        """
        dimension_names = {d.name for d in hierarchy.base_dimensions}
        existing_dimensions = await self.dimension_service.get_dimension_list_by_names(
            tenant_id=tenant_id, model_name=model_name, names=dimension_names
        )
        existing_dimension_names = {d.name for d in existing_dimensions}
        if dimension_names != existing_dimension_names:
            return dimension_names - existing_dimension_names
        return set()

    async def _get_absent_dimension_names_by_models(
        self, tenant_id: str, names_of_models: list[str], hierarchy: HierarchyMeta
    ) -> dict[str, set[str]]:
        """
        Возвращает словарь, где ключи — имена моделей, а значения — множества имён измерений,
        отсутствующих в каждой модели, но используемых в указанной иерархии.

        Для каждой модели из списка проверяется, какие измерения, задействованные в иерархии,
        в ней отсутствуют.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            names_of_models (list[str]): Список имён моделей, в которых проверяются измерения.
            hierarchy (HierarchyMeta): Метаданные иерархии, содержащие ссылки на измерения.

        Returns:
            dict[str, set[str]]: Словарь отсутствующих измерений по моделям.
                                 Например: {"model1": {"dim1", "dim2"}, "model2": {"dim3"}}
        """
        result = {}
        for model_name in names_of_models:
            absent_dimension_names = await self._get_absent_dimension_names(
                tenant_id=tenant_id, model_name=model_name, hierarchy=hierarchy
            )
            if absent_dimension_names:
                result[model_name] = absent_dimension_names
        return result

    @staticmethod
    def _raise_if_not_all_hierarchies_found(
        hierarchy_orm_list: list[HierarchyMeta], hierarchy_names: list[str]
    ) -> None:
        """
        Проверяет, что все запрошенные иерархии были найдены.

        Сравнивает количество найденных иерархий с ожидаемым по переданным именам.
        Если какие-либо иерархии не найдены, вызывает исключение HTTPException с кодом 404.

        Args:
            hierarchy_orm_list (list[HierarchyMeta]): Список найденных объектов иерархий.
            hierarchy_names (list[str]): Список имён запрошенных иерархий.

        Raises:
            HTTPException: Если количество найденных иерархий меньше, чем количество запрошенных (статус 404).

        Returns:
            None
        """
        hierarchy_orm_names_set = {hierarchy.name for hierarchy in hierarchy_orm_list}
        if set(hierarchy_names) != hierarchy_orm_names_set:
            raise ValueError(f"Hierarchies with names {set(hierarchy_names) - hierarchy_orm_names_set} not found")

    @staticmethod
    def _raise_if_not_all_models_found(model_orm_list: list[Model], names_of_models: list[str]) -> None:
        """
        Проверяет, что все запрошенные модели были найдены.

        Сравнивает количество найденных моделей с ожидаемым по переданным именам.
        Если какие-либо модели не найдены, вызывает исключение HTTPException с кодом 404.

        Args:
            model_orm_list (list[Model]): Список найденных объектов моделей.
            names_of_models (list[str]): Список имён запрошенных моделей.

        Raises:
            HTTPException: Если количество найденных моделей меньше, чем количество запрошенных (статус 404).

        Returns:
            None
        """
        model_orm_names_set = {model.name for model in model_orm_list}
        if set(names_of_models) != model_orm_names_set:
            raise ValueError(f"Models with names {set(names_of_models) - model_orm_names_set} not found")

    async def _get_error_response_if_unable_to_copy(
        self, hierarchy_orm: HierarchyMeta, names_of_models: list[str], tenant_id: str
    ) -> HierarchyCopyResponse | None:
        """
        Проверяет возможность копирования иерархии в указанные модели.

        Если иерархия не может быть скопирована (например, из-за отсутствия связанных измерений),
        формирует и возвращает объект ошибки `HierarchyCopyResponse`. В противном случае возвращает `None`.

        Args:
            hierarchy_orm (HierarchyMeta): Объект метаданных иерархии, которую пытаются скопировать.
            names_of_models (list[str]): Список имён моделей, в которые планируется копирование.
            tenant_id (str): Идентификатор арендатора (тенанта).

        Returns:
            HierarchyCopyResponse | None: Объект ответа с ошибкой, если копирование невозможно;
                                         иначе — None.
        """

        hierarchy_model_names = {model.name for model in hierarchy_orm.models}
        result = HierarchyCopyResponse(
            tenant=tenant_id,
            hierarchy_name=hierarchy_orm.name,
            names_of_models=names_of_models,
        )
        if set(names_of_models) & hierarchy_model_names:
            result.result = HierarchyCopyStatus.FAILURE
            result.comment = f"Hierarchy {hierarchy_orm.name} already exists in models {set(names_of_models) & hierarchy_model_names}"
            return result

        if absent_dimension_names := await self._get_absent_dimension_names_by_models(
            tenant_id=tenant_id, names_of_models=names_of_models, hierarchy=hierarchy_orm
        ):
            comment = ""
            for model_name, dimension_names in absent_dimension_names.items():
                comment += (
                    f"Dimensions: {dimension_names} with tenant_id {tenant_id}, model_name: {model_name} not found\n"
                )

            result.result = HierarchyCopyStatus.FAILURE
            result.comment = comment
            return result
        return None

    async def _copy_dimensions_to_destination_models(
        self, tenant_id: str, dimension_names: list[str], names_of_models: list[str]
    ) -> None:
        """
        Копирует указанные измерения в целевые модели тенанта.

        Для каждого из переданных имён измерений выполняется копирование структуры и данных
        во все модели из списка `names_of_models`, если измерение с таким именем ещё не существует.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            dimension_names (list[str]): Список имён измерений, которые необходимо скопировать.
            names_of_models (list[str]): Список имён моделей, в которые будут скопированы измерения.

        Returns:
            None

        Raises:
            HTTPException: Если возникает ошибка при копировании (например, измерение не найдено).
        """
        for dimension_name in dimension_names:
            await self.dimension_service.copy_model_dimension(
                tenant_id=tenant_id, model_names=names_of_models, name=dimension_name, copy_attributes=True
            )

    async def _copy_hierarchy_to_list_of_models(
        self, tenant_id: str, hierarchy_orm: HierarchyMeta, models: list[Model]
    ) -> HierarchyCopyResponse:
        """
        Копирует указанную иерархию в список целевых моделей.

        Для каждой модели выполняется:
        - Проверка наличия связанных измерений (при необходимости — их копирование).
        - Создание копии иерархии с привязкой к соответствующим измерениям модели.

        Возвращает отчёт о результате копирования, включая список успешно скопированных
        иерархий, а также ошибки, возникшие в ходе процесса.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            hierarchy_orm (HierarchyMeta): Объект метаданных исходной иерархии, которую необходимо скопировать.
            models (list[Model]): Список моделей, в которые будет скопирована иерархия.

        Returns:
            HierarchyCopyResponse: Объект с результатами копирования, включающий:
                                   - имена скопированных иерархий,
                                   - список ошибок (если были),
                                   - статус операции.
        """
        await self._copy_dimensions_to_destination_models(
            tenant_id=tenant_id,
            dimension_names=[d.name for d in hierarchy_orm.base_dimensions],
            names_of_models=[m.name for m in models],
        )
        error_response = await self._get_error_response_if_unable_to_copy(
            hierarchy_orm=hierarchy_orm, names_of_models=[m.name for m in models], tenant_id=tenant_id
        )
        if error_response:
            return error_response
        base_dimension_names = await self.hierarchy_repo.get_base_dimension_names_by_hierarchy_id(hierarchy_orm.id)
        base_dimension_name = next(dimension_tuple[0] for dimension_tuple in base_dimension_names if dimension_tuple[1])
        await self.assign_hierarchy_to_models(
            models=models, hierarchy=hierarchy_orm, base_dimension_name=base_dimension_name
        )
        return HierarchyCopyResponse(
            hierarchy_name=hierarchy_orm.name,
            names_of_models=[model.name for model in models],
            result=HierarchyCopyStatus.SUCCESS,
            tenant=tenant_id,
            comment=None,
        )

    async def copy_hierarchies_to_another_model(
        self,
        tenant_id: str,
        names_of_models: list[str],
        hierarchy_names: list[str],
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
        raise_if_error: bool = False,
    ) -> list[HierarchyCopyResponse]:
        """
        Копирует указанные иерархии в список целевых моделей.

        Для каждой иерархии:
        - Проверяется её наличие.
        - Проверяется наличие целевых моделей.
        - При необходимости — копируются связанные измерения.
        - Выполняется копирование иерархии в каждую из целевых моделей.

        Возвращает список отчётов о результатах копирования по каждой иерархии.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            hierarchy_names (list[str]): Список имён иерархий, которые необходимо скопировать.
            names_of_models (list[str]): Список имён моделей, в которые будут скопированы иерархии.

        Returns:
            list[HierarchyCopyResponse]: Список объектов с результатами копирования,
                                         по одному на каждую иерархию. Каждый результат содержит
                                         информацию об успешных копированиях и возникших ошибках.

        Raises:
            HTTPException: Если иерархии или модели не найдены (статус 404),
                           или если возникают ошибки при копировании.
        """
        try:
            await self.dimension_service.data_repository.create_not_virtual_dimensions(tenant_id, names_of_models)
            hierarchies = await self.hierarchy_repo.get_list(
                tenant_id=tenant_id,
                model_name=None,
                hierarchy_names=hierarchy_names,
                dimension_names=None,
            )
            self._raise_if_not_all_hierarchies_found(hierarchy_orm_list=hierarchies, hierarchy_names=hierarchy_names)
            models = await self.model_service.get_model_list_by_names(tenant_id=tenant_id, names=names_of_models)
            self._raise_if_not_all_models_found(model_orm_list=models, names_of_models=names_of_models)

            result = []
            for hierarchy in hierarchies:
                await self.hierarchy_history_repo.save_history(hierarchy, forced=True)
                result.append(
                    await self._copy_hierarchy_to_list_of_models(
                        tenant_id=tenant_id, hierarchy_orm=hierarchy, models=models
                    )
                )
                await self.hierarchy_history_repo.update_version(hierarchy, tenant_id=tenant_id)
        except Exception as exc:
            await self.hierarchy_repo.session.rollback()
            raise Exception(str(exc))
        else:
            await self.hierarchy_repo.session.commit()
            if send_to_aor:
                for hierarchy in hierarchies:
                    enriched_hierarchy = await self._enrich_hierarchy_orm_with_dimension_data(hierarchy)
                    await self.send_to_aor_by_name(
                        tenant_id, f"{enriched_hierarchy.base_dimension}__{enriched_hierarchy.name}"  # type: ignore
                    )
        return result

    @staticmethod
    async def _clear_hierarchy_cache(
        tenant_id: str, dimension_name: str, hierarchy_name: str, models: list[Model]
    ) -> None:
        """
        Очищает кэш иерархии для указанного тенанта, измерения и имени иерархии
        во всех переданных моделях.

        Используется после операций изменения или копирования иерархии,
        чтобы гарантировать актуальность данных при последующих запросах.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта).
            dimension_name (str): Имя измерения, к которому относится иерархия.
            hierarchy_name (str): Имя иерархии, кэш которой необходимо очистить.
            models (list[Model]): Список моделей, в которых нужно очистить кэш.

        Returns:
            None
        """
        await CacheRepository.clear_hierarchy_cache_by_name(tenant_id, dimension_name, hierarchy_name)
        for model in models:
            await CacheRepository.clear_hierarchy_by_dimension(tenant_id, model.name, dimension_name)
            await CacheRepository.clear_hierarchies_cache_by_model_name(tenant_id, model.name)
            await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id, model.name)

    async def _update_additional_dimensions_to_hierarchy(
        self, hierarchy: HierarchyMeta, tenant_id: str, model_name: str, additional_dimensions: list[str]
    ) -> None:
        """
        Обновляет список дополнительных измерений для указанной иерархии.

        Args:
            hierarchy (HierarchyMeta): Иерархия, для которой обновляется список дополнительных измерений.
            tenant_id (str): ID арендатора, в рамках которого находится иерархия.
            model_name (str): Имя модели, к которой относится иерархия.
            additional_dimensions (list[str]): Новый список наименований дополнительных измерений, которые будут установлены для иерархии.

        Returns:
            None: Метод не возвращает значения, он изменяет состояние иерархии.
        """
        existing_additional_dimensions = await self.hierarchy_repo.get_hierarchy_base_dimensions_by_hierarchy_id(
            hierarchy.id
        )
        existing_additional_dimensions_ids = {
            additional_dimension.dimension_name
            for additional_dimension in existing_additional_dimensions
            if not additional_dimension.is_base
        }

        base_dimension = next(
            base_dim.dimension_name for base_dim in existing_additional_dimensions if base_dim.is_base
        )

        new_claimed_additional_dimensions = set(additional_dimensions)
        dimensions_to_remove = existing_additional_dimensions_ids - set(additional_dimensions)

        if base_dimension in new_claimed_additional_dimensions:
            raise ValueError(
                f"Dimension {base_dimension} claimed as base dimension for hierarchy. "
                f"that means it cannot be used as additional dimension "
            )

        for additional_dimension in existing_additional_dimensions:
            if additional_dimension.dimension_name in dimensions_to_remove:
                await self.hierarchy_repo.session.delete(additional_dimension)

        dimensions_to_add = new_claimed_additional_dimensions - existing_additional_dimensions_ids
        dimensions_orm_to_add = await self.dimension_service.data_repository.get_list_of_orm_models(
            tenant_id, model_name, list(dimensions_to_add)
        )
        created_relations = []
        for dimension in dimensions_orm_to_add:
            created_relations.append(
                HierarchyBaseDimension(hierarchy_id=hierarchy.id, dimension_id=dimension.id, is_base=False)
            )
        self.hierarchy_repo.session.add_all(created_relations)
        await self.hierarchy_repo.session.flush()

    async def update_hierarchy_by_schema(
        self,
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        hierarchy: HierarchyEditRequest,
        hierarchy_name: str,
        send_to_aor: bool = True,
    ) -> HierarchyMetaOut:
        """
        Обновляет существующую иерархию на основе переданных изменений.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта), в рамках которого хранится иерархия.
            model_name (str): Имя модели, к которой относится иерархия.
            dimension_name (str): Имя измерения, связанное с иерархией.
            hierarchy (HierarchyEditRequest): Запрос на обновление иерархии, содержащий новые данные и изменения.
            hierarchy_name (str): Текущее имя иерархии, которое подлежит обновлению.

        Returns:
            HierarchyMetaOut: Объект с обновлёнными метаданными иерархии после успешного завершения операции.
        """
        try:
            hierarchy_orm = (
                await self.hierarchy_repo.get_list(
                    tenant_id=tenant_id,
                    model_name=model_name,
                    dimension_names=[dimension_name],
                    hierarchy_names=[hierarchy_name],
                )
            )[0]
            await self.hierarchy_history_repo.save_history(hierarchy_orm, edit_model=hierarchy.model_dump())
            if hierarchy.labels:
                hierarchy.labels = [label.model_dump() for label in hierarchy.labels]  # type: ignore[misc]
                add_missing_labels(hierarchy.labels, hierarchy_orm.name)  # type: ignore[arg-type]
            hierarchy_update_data = hierarchy.model_dump()
            additional_dimensions = hierarchy_update_data.pop("additional_dimensions")
            if additional_dimensions:
                await self._update_additional_dimensions_to_hierarchy(
                    hierarchy=hierarchy_orm,
                    tenant_id=tenant_id,
                    model_name=model_name,
                    additional_dimensions=additional_dimensions,
                )
            labels = hierarchy_update_data.pop("labels")
            self._update_hierarchy_labels(labels, hierarchy_orm)

            for key, value in hierarchy_update_data.items():
                if value is None:
                    continue
                setattr(hierarchy_orm, key, value)

            self.hierarchy_repo.session.add(hierarchy_orm)
            await self.hierarchy_history_repo.update_version(hierarchy_orm, tenant_id=tenant_id)
            await self.hierarchy_repo.session.flush()

            if self._is_pvd_meta_sync_enabled():
                await self.pvd_service.update_hierarchy_in_pvd(  # type: ignore[union-attr]
                    tenant_id=tenant_id,
                    model_name=model_name,
                    dimension_name=dimension_name,
                    hierarchy_name=hierarchy_orm.name,
                    commit=False,
                )

            await self.hierarchy_repo.session.commit()
            await self.hierarchy_repo.session.refresh(hierarchy_orm)
            hierarchy_orm = await self._enrich_hierarchy_orm_with_dimension_data(hierarchy_orm)

            await self._clear_hierarchy_cache(
                tenant_id=tenant_id,
                dimension_name=dimension_name,
                hierarchy_name=hierarchy_orm.name,
                models=hierarchy_orm.models,
            )
            result = HierarchyMetaOut.model_validate(hierarchy_orm)
            if send_to_aor:
                await self.create_and_send_command_to_aor_by_hierarchy(tenant_id, result)
            return result
        except Exception as exc:
            await self.hierarchy_repo.session.rollback()
            raise Exception(str(exc))

    def _update_hierarchy_labels(self, labels: list[dict[str, Any]], hierarchy_orm: HierarchyMeta) -> None:
        """
        Обновляет метки (labels) у переданной иерархии на основе предоставленного списка словарей.

        Метод сливает новые метки с существующими: обновляет значения по ключам, добавляет новые пары ключ-значение.
        Если список меток пуст — никаких изменений не происходит.

        Args:
            labels (list[dict[str, Any]]): Список словарей с метками для обновления. Ожидается один элемент.
            hierarchy_orm (HierarchyMeta): Объект иерархии, метки которой необходимо обновить.

        Returns:
            None

        Note:
            Метод изменяет объект `hierarchy_orm` напрямую (in-place), не возвращая новый объект.
        """
        if not labels and hierarchy_orm.labels:
            return
        add_missing_labels(labels, hierarchy_orm.name)

        orm_labels = convert_labels_list_to_orm(labels, HierarchyLabel)
        for label in orm_labels:
            label.hierarchy_id = hierarchy_orm.id
        hierarchy_orm.labels = orm_labels
        self.hierarchy_repo.session.add_all(orm_labels)

    async def delete_hierarchy(
        self,
        tenant_id: str,
        model_name: str,
        dimension_name: str,
        hierarchy_name: str,
        send_to_aor: bool = True,
        check_possible_delete: bool = True,
    ) -> None:
        """
        Удаляет иерархию по указанным параметрам.

        Args:
            tenant_id (str): Идентификатор арендатора (тенанта), в рамках которого хранитс
            model_name (str): Имя модели, к которой относится удаляемая иерархия.
            dimension_name (str): Имя измерения, связанного с удаляемой иерархией.
            hierarchy_name (str): Имя иерархии, которая будет удалена.

        Returns:
            None: Метод не возвращает значения, он удаляет иерархию.
        """
        hierarchy_model_names: list[str] = []
        command: Optional[CreateAorCommand] = None
        try:
            hierarchy_orm_list = await self.hierarchy_repo.get_list(
                tenant_id=tenant_id,
                model_name=model_name,
                dimension_names=[dimension_name],
                hierarchy_names=[hierarchy_name],
            )
            if not hierarchy_orm_list:
                raise ValueError(f"hierarchy with name: {hierarchy_name} not found")

            hierarchy_to_delete = hierarchy_orm_list[0]
            hierarchy_model_names = [model.name for model in hierarchy_to_delete.models]
            hierarchy_to_delete = await self._enrich_hierarchy_orm_with_dimension_data(hierarchy_to_delete)
            if self._is_pvd_meta_sync_enabled():
                with suppress(ValueError):
                    await self.pvd_service.delete_hierarchy_from_pvd(  # type: ignore[union-attr]
                        tenant_id=tenant_id,
                        model_name=model_name,
                        dimension_name=dimension_name,
                        hierarchy_name=hierarchy_name,
                        commit=False,
                    )

            command = await self.create_and_send_command_to_aor_by_hierarchy(
                tenant_id=tenant_id,
                hierarchy=HierarchyMetaOut.model_validate(hierarchy_to_delete),
                deleted=True,
                send_command=False,
            )
            await self.hierarchy_history_repo.save_history(hierarchy_to_delete, deleted=True)
            await self.hierarchy_repo.delete_by_id(hierarchy_to_delete.id, model_name=model_name)
            await self.hierarchy_repo.session.commit()
        except Exception as exc:
            await self.hierarchy_repo.session.rollback()
            raise Exception(str(exc))

        await CacheRepository.clear_hierarchy_cache_by_name(tenant_id, dimension_name, hierarchy_name)
        for model_name in hierarchy_model_names:
            await CacheRepository.clear_hierarchy_by_dimension(tenant_id, model_name, dimension_name)
            await CacheRepository.clear_hierarchies_cache_by_model_name(tenant_id, model_name)
            await CacheRepository.clear_data_storages_cache_by_model_name(tenant_id, model_name)
        if send_to_aor:
            await self.aor_client.send_request(command)

    async def create_and_send_command_to_aor_by_hierarchy(
        self,
        tenant_id: str,
        hierarchy: HierarchyMetaOut,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        send_command: bool = True,
        depends_no_attrs_versions: bool = False,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> Optional[CreateAorCommand]:
        try:
            if not hierarchy.base_dimension:
                raise ValueError("Hierarchy has not base dimension")
            data_json = JsonData(
                is_deleted=deleted, tenant=tenant_id, data_json=hierarchy.model_dump(mode="json", by_alias=True)
            )
            parents = (
                await self.aor_repository.get_hierarchy_parents_by_schema(tenant_id, hierarchy) if with_parents else []
            )
            for parent in parents:
                parent.parent_name += parent_name_suffix
                parent.parent_external_id += parent_name_suffix
                parent.parent_version += parent_version_suffix
            if depends_no_attrs_versions:
                for parent in parents:
                    if parent.parent_type != AorType.DIMENSION:
                        continue
                    if not parent_version_suffix:
                        parent.parent_version += "-no-attrs"
            command = {
                "type": AorType.HIERARCHY,
                "name": hierarchy.aor_name + name_suffix,
                "data_json": data_json,
                "description": hierarchy.aor_name,
                "version": (
                    str(hierarchy.version) + version_suffix
                    if not deleted
                    else f"{hierarchy.version}-deleted" + version_suffix
                ),
                "external_object_id": hierarchy.aor_name + name_suffix,
                "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
                "parents": parents,
                "space_id": custom_uuid
                or (
                    await self.aor_repository.get_dimension_aor_space_by_names(tenant_id, [hierarchy.base_dimension])
                ).get(hierarchy.base_dimension),
            }
            command_model = CreateAorCommand.model_validate(command)
        except Exception:
            logger.exception("Create command model failed.")
            command_model = None
        if send_command and command_model:
            await self.aor_client.send_request(command_model)
        return command_model

    async def send_to_aor_by_name(
        self,
        tenant_id: str,
        name: str,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        with_parents: bool = True,
        dim_with_attributes: bool = True,
        depends_no_attrs_versions: bool = False,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> None:
        hierarchy = await self.get_hierarchy_by_hierarchy_name_and_sep(tenant_id, name)
        await self.create_and_send_command_to_aor_by_hierarchy(
            tenant_id,
            hierarchy,
            deleted,
            custom_uuid,
            with_parents,
            depends_no_attrs_versions=depends_no_attrs_versions,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )

    def __repr__(self) -> str:
        return "HierarchyService"
