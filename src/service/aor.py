"""
Сервис интеграции с АОР.
"""

import copy
from typing import Any, Callable, Coroutine, Optional, Protocol
from uuid import UUID

from py_common_lib.logger import EPMPYLogger
from pydantic import BaseModel

from src.integration.aor.model import AorType, PushAorCommand
from src.models.aor import AORCreateRequestTypesEnum, CommandEnum, CreateAorRequest, CreateModelAorRequest
from src.models.composite import Composite, CompositeCreateRequest, CompositeEditRequest
from src.models.copy_model import DetailsObjectCopyReponse
from src.models.data_storage import DataStorage, DataStorageCreateRequest, DataStorageEditRequest, DataStorageEnum
from src.models.database import Database, DatabaseCreateRequest, DatabaseEditRequest
from src.models.dimension import Dimension, DimensionCreateRequest, DimensionEditRequest
from src.models.hierarchy import HierarchyCopyResponse, HierarchyCreateRequest, HierarchyEditRequest, HierarchyMetaOut
from src.models.measure import Measure, MeasureCreateRequest, MeasureEditRequest
from src.models.model import Model, ModelCreateRequest, ModelEditRequest
from src.service.composite import CompositeService
from src.service.data_storage import DataStorageService
from src.service.database import DatabaseService
from src.service.dimension import DimensionService
from src.service.graph import GraphService
from src.service.hierarchy import HierarchyService
from src.service.measure import MeasureService
from src.service.model import ModelService
from src.service.utils import get_diff_lst
from src.utils.schema_override import (
    apply_schema_override_to_database_objects,
    apply_schema_override_to_model_payload,
    get_model_schema_override,
)

logger = EPMPYLogger(__name__)


class ServiceWithAorIntegration(Protocol):
    """
    Интерфейс для классов, которые имеют интеграцию с АОР
    """

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
        pass


class DatabaseObjectAorAdapter:
    """Адаптер для отправки database_object в AOR."""

    def __init__(self, datastorage_service: DataStorageService) -> None:
        self._datastorage_service = datastorage_service

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
        await self._datastorage_service.send_database_object_to_aor_by_name(
            tenant_id=tenant_id,
            name=name,
            deleted=deleted,
            custom_uuid=custom_uuid,
            with_parents=with_parents,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )


DatabaseUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, Database]],
    Callable[..., Coroutine[Any, Any, None]],
    Callable[..., Coroutine[Any, Any, Database]],
]

ModelUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, Model]],
    Callable[..., Coroutine[Any, Any, None]],
    Callable[..., Coroutine[Any, Any, Model]],
]

DataStorageUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, DataStorage]],
    Callable[..., Coroutine[Any, Any, None]],
    Callable[..., Coroutine[Any, Any, DataStorage]],
    Callable[..., Coroutine[Any, Any, tuple[DetailsObjectCopyReponse, bool]]],
]

DimensionUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, Dimension | None]],
    Callable[..., Coroutine[Any, Any, bool]],
    Callable[..., Coroutine[Any, Any, Dimension | None]],
    Callable[..., Coroutine[Any, Any, tuple[DetailsObjectCopyReponse, bool]]],
]

CompositeUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, Composite]],
    Callable[..., Coroutine[Any, Any, None]],
    Callable[..., Coroutine[Any, Any, Composite]],
    Callable[..., Coroutine[Any, Any, tuple[DetailsObjectCopyReponse, bool]]],
]

MeasureUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, Measure]],
    Callable[..., Coroutine[Any, Any, None]],
    Callable[..., Coroutine[Any, Any, Measure]],
    Callable[..., Coroutine[Any, Any, tuple[DetailsObjectCopyReponse, bool]]],
]

HierarchyUpdateMethodsTuple = tuple[
    Callable[..., Coroutine[Any, Any, HierarchyMetaOut]],
    Callable[..., Coroutine[Any, Any, None]],
    Callable[..., Coroutine[Any, Any, HierarchyMetaOut]],
    Callable[..., Coroutine[Any, Any, list[HierarchyCopyResponse]]],
]


pydantic_model_mappings: dict[AorType, tuple[type[BaseModel], type[BaseModel]]] = {
    AorType.DATABASE: (DatabaseCreateRequest, DatabaseEditRequest),
    AorType.MODEL: (ModelCreateRequest, ModelEditRequest),
    AorType.MEASURE: (MeasureCreateRequest, MeasureEditRequest),
    AorType.DIMENSION: (DimensionCreateRequest, DimensionEditRequest),
    AorType.DATASTORAGE: (DataStorageCreateRequest, DataStorageEditRequest),
    AorType.COMPOSITE: (CompositeCreateRequest, CompositeEditRequest),
    AorType.HIERARCHY: (HierarchyCreateRequest, HierarchyEditRequest),
}


class AorService:
    """
    Сервис для двустороннего общения с AOR

    Данный класс предназначен для централизованной обработки различных типов запросов
    через делегирование конкретных операций соответствующим сервисам.
    В конструкторе определяется соответствие между типами запросов и соответствующими сервисами.
    Это позволяет гибко расширять функциональность путем добавления новых сервисов
    и привязки их к новым типам запросов.
    """

    def __init__(
        self,
        database_service: DatabaseService,
        model_service: ModelService,
        measure_service: MeasureService,
        dimension_service: DimensionService,
        datastorage_service: DataStorageService,
        composite_service: CompositeService,
        hierarchy_service: HierarchyService,
    ) -> None:
        self.graph_service = GraphService()
        self.database_service = database_service
        self.model_service = model_service
        self.hierarchy_service = hierarchy_service
        self.measure_service = measure_service
        self.dimension_service = dimension_service
        self.datastorage_service = datastorage_service
        self.compositre_service = composite_service
        self.database_object_service = DatabaseObjectAorAdapter(self.datastorage_service)
        self.aor_type_service_mapping: dict[AORCreateRequestTypesEnum, ServiceWithAorIntegration] = {
            AORCreateRequestTypesEnum.DATABASE: self.database_service,
            AORCreateRequestTypesEnum.MODEL: self.model_service,
            AORCreateRequestTypesEnum.MEASURE: self.measure_service,
            AORCreateRequestTypesEnum.DIMENSION: self.dimension_service,
            AORCreateRequestTypesEnum.DATASTORAGE: self.datastorage_service,
            AORCreateRequestTypesEnum.COMPOSITE: self.compositre_service,
            AORCreateRequestTypesEnum.HIERARCHY: self.hierarchy_service,
            AORCreateRequestTypesEnum.DATABASEOBJECT: self.database_object_service,
        }

    async def send_to_aor(self, tenant_id: str, aor_request: CreateAorRequest) -> None:
        """
        Отправляет запрос на создание объекта в AOR.

        Args:
            tenant_id (str): Идентификатор арендатора/организации.
            aor_request (CreateAorRequest): Запрос на создание нового объекта AOR, содержащий следующие поля:
                type (str): Тип создаваемого объекта AOR.
                name (str): Имя создаваемого объекта AOR.
                is_deleted (bool): Флаг удаления объекта AOR (если true — объект считается удалённым).
                space_id (UUID): Уникальный идентификатор пространства в AOR.
                with_parents (bool): Флаг отправки родительских объктов.

        """
        await self.aor_type_service_mapping[aor_request.type].send_to_aor_by_name(
            tenant_id=tenant_id,
            name=aor_request.name,
            deleted=aor_request.is_deleted,
            custom_uuid=aor_request.space_id,
            with_parents=aor_request.with_parents,
            dim_with_attributes=aor_request.dim_with_attributes,
            depends_no_attrs_versions=aor_request.depends_no_attrs_versions,
            version_suffix=aor_request.version_suffix,
            parent_version_suffix=aor_request.parent_version_suffix,
            name_suffix=aor_request.name_suffix,
            parent_name_suffix=aor_request.parent_name_suffix,
        )
        return None

    def pop_not_processed_fields(self, obj: dict) -> None:
        """
        Удаляет поля, которые не должны учитываться при деплое объектов.

        Args:
            obj (dict): Объект, из которого нужно удалить поля."""
        obj.pop("pv_dictionary", None)
        obj.pop("pvDictionary", None)

    def __get_models_with_schema_override(
        self,
        aor_type: AorType,
        tenant_id: str,
        model_names: list[str],
    ) -> list[str]:
        """Возвращает модели, для которых задан env-override схемы при деплое объекта."""
        if aor_type not in {AorType.DATASTORAGE, AorType.COMPOSITE}:
            return []
        models_with_override = [
            model_name for model_name in model_names if get_model_schema_override(tenant_id, model_name)
        ]
        return models_with_override

    def __apply_schema_override_to_payload(
        self,
        push_command: PushAorCommand,
        model_name: Optional[str] = None,
    ) -> bool:
        """
        Применяет override схемы к payload деплоя.

        Для `MODEL` обновляет поле схемы модели, для `DATASTORAGE/COMPOSITE`
        обновляет `schemaName/schema_name` в `dbObjects`.
        """
        payload = push_command.data_json.data_json
        tenant_id = push_command.data_json.tenant
        if push_command.type in {AorType.DATASTORAGE, AorType.COMPOSITE} and not model_name:
            payload_models = payload.get("models")
            if isinstance(payload_models, list) and payload_models:
                first_model = payload_models[0]
                if isinstance(first_model, dict):
                    model_name = first_model.get("name")
                elif isinstance(first_model, str):
                    model_name = first_model
        if push_command.type == AorType.MODEL:
            result = apply_schema_override_to_model_payload(payload, tenant_id)
            return result
        if push_command.type not in {AorType.DATASTORAGE, AorType.COMPOSITE} or not model_name:
            return False

        schema_override = get_model_schema_override(tenant_id, model_name)
        if not schema_override:
            return False

        database_objects = payload.get("dbObjects")
        if database_objects is None:
            database_objects = payload.get("database_objects")
        if not isinstance(database_objects, list):
            return False
        before_schemas = [
            obj.get("schemaName") if isinstance(obj, dict) else getattr(obj, "schema_name", None)
            for obj in database_objects
        ]
        result = apply_schema_override_to_database_objects(database_objects, schema_override)
        after_schemas = [
            obj.get("schemaName") if isinstance(obj, dict) else getattr(obj, "schema_name", None)
            for obj in database_objects
        ]
        return result

    async def __get_commands_not_linked_to_model(
        self, command: PushAorCommand
    ) -> list[tuple[CommandEnum, Optional[str]]]:
        """
        Возвращает список команд (create/update/copy/delete), для объектов, которые не имеют привязку к моделям
        (DATABASE, MODEL).

        Args:
            command (PushAorCommand): Команда, содержащая тип объекта и новые данные JSON на обновление
        Returns:
            List[Tuple[CommandEnum, Optional[str]]]: Список кортежей, где первый элемент — команда
                                                (тип CommandEnum), второй — имя модели (если применимо)

        Raises:
            ValueError: Если передан неизвестный тип объекта
        """
        getters: dict[AorType, Callable] = {
            AorType.DATABASE: self.database_service.get_database_by_name_or_null,
            AorType.MODEL: self.model_service.get_model_by_name_or_null,
        }

        not_linked_to_model_object = await getters[command.type](
            command.data_json.tenant, command.data_json.data_json["name"]
        )
        if not_linked_to_model_object is None and command.data_json.is_deleted:
            logger.info(
                "Delete flag enabled for %s %s but object not found",
                command.type,
                command.data_json.data_json["name"],
            )
            return []
        if not_linked_to_model_object is None:
            logger.info(
                "Create %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
            return [(CommandEnum.CREATE, None)]

        obj_dict = self.__build_comparable_payload(not_linked_to_model_object.model_dump(by_alias=True, mode="json"))

        if command.data_json.is_deleted:
            logger.info(
                "Delete flag enabled for %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
            return [(CommandEnum.DELETE, None)]
        command_payload = self.__build_comparable_payload(command.data_json.data_json)
        if command_payload != obj_dict:
            logger.info(
                "Content updated for %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
            return [(CommandEnum.UPDATE, None)]
        logger.info(
            "Content not updated for %s %s",
            command.type,
            command.data_json.data_json["name"],
        )
        return []

    def __clear_uncomparable_fields(self, obj_dict: dict) -> None:
        """Очистка полей, которые не участвуют в сравнении."""
        obj_dict.pop("dbObjects", None)
        obj_dict.pop("version", None)
        obj_dict.pop("updatedAt", None)
        obj_dict.pop("updatedBy", None)
        obj_dict.pop("models", None)
        if "fields" in obj_dict:
            for field in obj_dict["fields"]:
                field.pop("sqlColumnType", None)

    def __build_comparable_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Возвращает копию payload, очищенную от несравниваемых полей."""
        comparable_payload = copy.deepcopy(payload)
        self.__clear_uncomparable_fields(comparable_payload)
        return comparable_payload

    def __append_schema_override_update_commands(
        self,
        result: list[tuple[CommandEnum, Optional[str]]],
        models_with_schema_override: list[str],
        skip_models: Optional[set[str]] = None,
    ) -> None:
        """Добавляет UPDATE-команды для моделей с override схемы, если команды ещё не запланированы."""
        skip_models = skip_models or set()
        for model in models_with_schema_override:
            if model in skip_models:
                continue
            if (CommandEnum.DELETE, model) in result or (CommandEnum.UPDATE, model) in result:
                continue
            result.append((CommandEnum.UPDATE, model))

    async def __get_commands_linked_to_model(self, command: PushAorCommand) -> list[tuple[CommandEnum, Optional[str]]]:
        """
        Возвращает список команд (create/update/copy/delete), для объектов, которые имеют привязку к моделям
        (MEASURE, DIMENSION, DATASTORAGE, COMPOSITE).

        Args:
            command (PushAorCommand): Команда, содержащая тип объекта и новые данные JSON на обновление
        Returns:
            List[Tuple[CommandEnum, Optional[str]]]: Список кортежей, где первый элемент — команда
                                                (тип CommandEnum), второй — имя модели (если применимо)

        Raises:
            ValueError: Если передан неизвестный тип объекта или объект имеет некорректную структуру моделей
        """
        if command.type == AorType.DATABASEOBJECT:
            obj_name = command.data_json.data_json.get("name", command.name)
            if command.data_json.is_deleted:
                logger.info("Delete flag enabled for %s %s", command.type, obj_name)
                return [(CommandEnum.DELETE, None)]
            return [(CommandEnum.UPDATE, None)]
        getters: dict[AorType, Callable] = {
            AorType.MEASURE: self.measure_service.get_measure_by_measure_name_or_null,
            AorType.DIMENSION: self.dimension_service.get_dimension_by_dimension_name_or_null,
            AorType.DATASTORAGE: self.datastorage_service.get_data_storage_by_name_or_null,
            AorType.COMPOSITE: self.compositre_service.get_composite_by_name_or_null,
            AorType.HIERARCHY: self.hierarchy_service.get_hierarchy_by_hierarchy_name_and_sep_or_null,
        }
        if getters.get(command.type) is None:
            raise ValueError(f"Unknown type: {command.type}")
        command_payload = copy.deepcopy(command.data_json.data_json)
        new_models = command_payload.pop("models", [])
        if not new_models:
            raise ValueError("Deploy object MEASURE, DIMENSION, DATASTORAGE or Composite must have models")
        if isinstance(new_models[0], dict):
            new_models = [model["name"] for model in new_models]
        _filtred_models = []
        for new_model in new_models:
            new_model_obj = await self.model_service.get_model_by_name_or_null(command.data_json.tenant, new_model)
            if new_model_obj:
                _filtred_models.append(new_model)
        new_models = _filtred_models
        models_with_schema_override = self.__get_models_with_schema_override(
            command.type,
            command.data_json.tenant,
            new_models,
        )
        if not new_models:
            raise ValueError("Deploy object MEASURE, DIMENSION, DATASTORAGE or Composite must have models")
        linked_to_model_object = await getters[command.type](
            tenant_id=command.data_json.tenant,
            name=(
                command.data_json.data_json["name"]
                if command.type != AorType.HIERARCHY
                else command.data_json.data_json["aorName"]
            ),
            model_name=None,
        )
        if linked_to_model_object is None and command.data_json.is_deleted:
            logger.info(
                "Delete flag enabled for %s %s but object not found",
                command.type,
                command.data_json.data_json["name"],
            )
            return []
        if linked_to_model_object is None:
            logger.info(
                "Create %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
            result = [(CommandEnum.CREATE, new_models[0])]
            if len(new_models) > 1:
                for model in new_models[1:]:
                    result.append(
                        (
                            CommandEnum.COPY,
                            model,
                        )
                    )
            self.__append_schema_override_update_commands(
                result,
                models_with_schema_override,
                skip_models={new_models[0]},
            )
            return result
        obj_dict = linked_to_model_object.model_dump(by_alias=True, mode="json")
        original_models = obj_dict.pop("models", [])
        if not original_models:
            raise ValueError("Prev object MEASURE, DIMENSION, HIERARCHY, DATASTORAGE or Composite must have models")
        if isinstance(original_models[0], dict):
            original_models = [model["name"] for model in original_models]
        obj_dict = self.__build_comparable_payload(obj_dict)
        command_payload = self.__build_comparable_payload(command_payload)
        deleted_models = get_diff_lst(original_models, new_models)
        created_models = get_diff_lst(new_models, original_models)
        if command.data_json.is_deleted:
            logger.info(
                "Delete flag enabled for %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
            return [(CommandEnum.DELETE, original_models[0])]

        result = []
        if created_models:
            logger.info(
                "Models for %s %s updated. prev_models=%s, new_models=%s Copy to %s",
                command.type,
                command.data_json.data_json["name"],
                original_models,
                new_models,
                created_models,
            )
            result.extend([(CommandEnum.COPY, model) for model in created_models])
        if deleted_models:
            logger.info(
                "Models for %s %s updated. prev_models=%s, new_models=%s Delete models %s",
                command.type,
                command.data_json.data_json["name"],
                original_models,
                new_models,
                deleted_models,
            )
            result.extend([(CommandEnum.DELETE, model) for model in deleted_models])
        if command_payload != obj_dict:
            logger.info(
                "Content updated for %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
            result.append((CommandEnum.UPDATE, new_models[0]))
        else:
            logger.info(
                "Content not updated for %s %s",
                command.type,
                command.data_json.data_json["name"],
            )
        self.__append_schema_override_update_commands(result, models_with_schema_override)
        return result

    async def __get_commands_by_push_command(self, command: PushAorCommand) -> list[tuple[CommandEnum, Optional[str]]]:
        logger.debug("Parse deploy package and create commands")
        obj_name = command.data_json.data_json.get("name", command.name)
        if command.type in {AorType.DATABASE, AorType.MODEL}:
            commands = await self.__get_commands_not_linked_to_model(command)
            logger.info("Commands for %s %s: %s", command.type, obj_name, commands)
            return commands

        commands = await self.__get_commands_linked_to_model(command)
        logger.info("Commands for %s %s: %s", command.type, obj_name, commands)
        return commands

    async def __update_not_linked_to_model_objects(
        self, push_command: PushAorCommand, command: tuple[CommandEnum, Optional[str]]
    ) -> None:
        """
        Обновляет объекты (DATABASE, MODEL), которые не привязаны к моделям,
        одним из 3 способов (`CREATE`, `UPDATE`, `DELETE`), используя сервисы и методы соответсвующего типа.

        Args:
            push_command (PushAorCommand): Объект командной структуры, содержащий тип объекта
                                            и необходимую информацию для выполнения операции.
            command (tuple[CommandEnum, Optional[str]]): Команда, указывающая действие над объектом
                                                        и модель, которому привязан объект.

        Raises:
            ValueError: Если в команде не указана целевая модель (`model_name`)
        """
        if push_command.type not in {AorType.DATABASE, AorType.MODEL}:
            raise ValueError(f"Bad command type for not linked to model objects: {push_command.type}")
        updaters: dict[AorType, DatabaseUpdateMethodsTuple | ModelUpdateMethodsTuple] = {
            AorType.DATABASE: (
                self.database_service.create_database_by_schema,
                self.database_service.delete_database_by_name,
                self.database_service.update_database_by_name_and_schema,
            ),
            AorType.MODEL: (
                self.model_service.create_model_by_schema,
                self.model_service.delete_model_by_name,
                self.model_service.update_model_by_name_and_schema,
            ),
        }
        if command[0] == CommandEnum.CREATE:
            logger.info(
                "Create %s %s.%s",
                push_command.type,
                push_command.data_json.tenant,
                push_command.data_json.data_json["name"],
            )
            self.__apply_schema_override_to_payload(push_command)
            CreatePydanticModel = pydantic_model_mappings[push_command.type][0]
            instance_create_pydantic_model: Any = CreatePydanticModel.model_validate(push_command.data_json.data_json)
            create_method = updaters[push_command.type][0]
            await create_method(
                push_command.data_json.tenant,
                instance_create_pydantic_model,
                send_to_aor=False,
            )
        elif command[0] == CommandEnum.UPDATE:
            logger.info(
                "Update %s %s.%s",
                push_command.type,
                push_command.data_json.tenant,
                push_command.data_json.data_json["name"],
            )
            self.__apply_schema_override_to_payload(push_command)
            UpdatePydanticModel = pydantic_model_mappings[push_command.type][1]
            instance_update_pydantic_model: Any = UpdatePydanticModel.model_validate(push_command.data_json.data_json)
            update_method = updaters[push_command.type][2]
            await update_method(
                push_command.data_json.tenant,
                push_command.data_json.data_json["name"],
                instance_update_pydantic_model,
                send_to_aor=False,
            )
        elif command[0] == CommandEnum.DELETE:
            logger.info(
                "Delete %s %s.%s",
                push_command.type,
                push_command.data_json.tenant,
                push_command.data_json.data_json["name"],
            )
            delete_method = updaters[push_command.type][1]
            await delete_method(
                push_command.data_json.tenant,
                push_command.data_json.data_json["name"],
                send_to_aor=False,
            )
        logger.info(
            "Command for %s %s.%s completed",
            push_command.type,
            push_command.data_json.tenant,
            push_command.data_json.data_json["name"],
        )

    async def __update_linked_to_model_objects(
        self, push_command: PushAorCommand, command: tuple[CommandEnum, Optional[str]]
    ) -> None:
        """
        Обновляет объекты (Data Storage, Dimensions, Composites, Measures), которые имеют привязку к моделям,
        одним из 4 способов (`CREATE`, `UPDATE`, `DELETE`, `COPY`), используя сервисы и методы соответсвующего типа.

        Args:
            push_command (PushAorCommand): Объект командной структуры, содержащий тип объекта
                                            и необходимую информацию для выполнения операции.
            command (tuple[CommandEnum, Optional[str]]): Команда, указывающая действие над объектом
                                                        и модель, которому привязан объект.

        Raises:
            ValueError: Если в команде не указана целевая модель (`model_name`)
        """
        if push_command.type == AorType.DATABASEOBJECT:
            await self.datastorage_service.deploy_database_object_from_aor(push_command)
            return None
        updaters: dict[
            AorType,
            DataStorageUpdateMethodsTuple
            | DimensionUpdateMethodsTuple
            | CompositeUpdateMethodsTuple
            | MeasureUpdateMethodsTuple
            | HierarchyUpdateMethodsTuple,
        ] = {
            AorType.DATASTORAGE: (
                self.datastorage_service.create_data_storage_by_schema,
                self.datastorage_service.delete_data_storage_by_name,
                self.datastorage_service.update_data_storage_by_name_and_schema,
                self.datastorage_service.copy_model_data_storages,
            ),
            AorType.DIMENSION: (
                self.dimension_service.create_dimension_by_schema,
                self.dimension_service.delete_dimension_by_name,
                self.dimension_service.update_dimension_by_name_and_schema,
                self.dimension_service.copy_model_dimensions,
            ),
            AorType.COMPOSITE: (
                self.compositre_service.create_composite_by_schema,
                self.compositre_service.delete_composite_by_name,
                self.compositre_service.update_composite_by_name_and_schema,
                self.compositre_service.copy_model_composites,
            ),
            AorType.MEASURE: (
                self.measure_service.create_measure_by_schema,
                self.measure_service.delete_measure_by_name,
                self.measure_service.update_measure_by_name_and_schema,
                self.measure_service.copy_model_measures,
            ),
            AorType.HIERARCHY: (
                self.hierarchy_service.create_hierarchy_by_schema,
                self.hierarchy_service.delete_hierarchy,
                self.hierarchy_service.update_hierarchy_by_schema,
                self.hierarchy_service.copy_hierarchies_to_another_model,
            ),
        }
        if not command[1]:
            raise ValueError("Model name must be not None for DATASTORAGE, DIMENSION, COMPOSITE or MEASURE objects")
        if command[0] == CommandEnum.CREATE:
            if push_command.type == AorType.DATASTORAGE:
                await self.datastorage_service.drop_dependent_views_for_datastorage(
                    tenant_id=push_command.data_json.tenant,
                    model_name=command[1],
                    data_storage_name=push_command.data_json.data_json["name"],
                )
            logger.info(
                "Create %s %s.%s.%s",
                push_command.type,
                push_command.data_json.tenant,
                command[1],
                push_command.data_json.data_json["name"],
            )
            self.__apply_schema_override_to_payload(push_command, command[1])
            CreatePydanticModel = pydantic_model_mappings[push_command.type][0]
            instance_create_pydantic_model: Any = CreatePydanticModel.model_validate(push_command.data_json.data_json)
            create_method = updaters[push_command.type][0]
            if push_command.type in {AorType.DATASTORAGE, AorType.DIMENSION}:
                await create_method(
                    push_command.data_json.tenant,
                    command[1],
                    instance_create_pydantic_model,
                    if_not_exists=True,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
            elif push_command.type == AorType.COMPOSITE:
                await create_method(
                    push_command.data_json.tenant,
                    command[1],
                    instance_create_pydantic_model,
                    replace=True,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
            elif push_command.type == AorType.HIERARCHY:
                dimension_name, _ = self.hierarchy_service.get_dimension_name_and_hierarchy_name_by_name(
                    push_command.data_json.data_json["aorName"]
                )
                await create_method(
                    push_command.data_json.tenant,
                    command[1],
                    dimension_name,
                    instance_create_pydantic_model,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
            else:
                await create_method(
                    push_command.data_json.tenant,
                    command[1],
                    instance_create_pydantic_model,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
        elif command[0] == CommandEnum.UPDATE:
            if push_command.type == AorType.DATASTORAGE:
                await self.datastorage_service.drop_dependent_views_for_datastorage(
                    tenant_id=push_command.data_json.tenant,
                    model_name=command[1],
                    data_storage_name=push_command.data_json.data_json["name"],
                )
            logger.info(
                "Update %s %s.%s.%s",
                push_command.type,
                push_command.data_json.tenant,
                command[1],
                push_command.data_json.data_json["name"],
            )
            self.__apply_schema_override_to_payload(push_command, command[1])
            UpdatePydanticModel = pydantic_model_mappings[push_command.type][1]
            instance_update_pydantic_model: Any = UpdatePydanticModel.model_validate(push_command.data_json.data_json)
            update_method = updaters[push_command.type][2]
            if push_command.type in {AorType.MEASURE, AorType.COMPOSITE}:
                await update_method(
                    push_command.data_json.tenant,
                    command[1],
                    push_command.data_json.data_json["name"],
                    instance_update_pydantic_model,
                    send_to_aor=False,
                )
            elif push_command.type not in {AorType.DATASTORAGE, AorType.DIMENSION, AorType.HIERARCHY}:
                await update_method(
                    push_command.data_json.tenant,
                    command[1],
                    push_command.data_json.data_json["name"],
                    instance_update_pydantic_model,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
            elif push_command.type == AorType.HIERARCHY:
                dimension_name, hierarchy_name = self.hierarchy_service.get_dimension_name_and_hierarchy_name_by_name(
                    push_command.data_json.data_json["aorName"]
                )
                await update_method(
                    push_command.data_json.tenant,
                    command[1],
                    dimension_name,
                    instance_update_pydantic_model,
                    hierarchy_name,
                    send_to_aor=False,
                )
            else:
                await update_method(
                    push_command.data_json.tenant,
                    command[1],
                    push_command.data_json.data_json["name"],
                    instance_update_pydantic_model,
                    send_to_aor=False,
                    enable_delete_not_empty=True,
                )
        elif command[0] == CommandEnum.DELETE:
            if push_command.type == AorType.DATASTORAGE:
                await self.datastorage_service.drop_dependent_views_for_datastorage(
                    tenant_id=push_command.data_json.tenant,
                    model_name=command[1],
                    data_storage_name=push_command.data_json.data_json["name"],
                )
            logger.info(
                "Delete %s %s.%s.%s",
                push_command.type,
                push_command.data_json.tenant,
                command[1],
                push_command.data_json.data_json["name"],
            )
            delete_method = updaters[push_command.type][1]
            if push_command.type not in {AorType.DATASTORAGE, AorType.DIMENSION, AorType.HIERARCHY}:
                await delete_method(
                    push_command.data_json.tenant,
                    command[1],
                    push_command.data_json.data_json["name"],
                    if_exists=True,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
            elif push_command.type == AorType.HIERARCHY:
                dimension_name, hierarchy_name = self.hierarchy_service.get_dimension_name_and_hierarchy_name_by_name(
                    push_command.data_json.data_json["aorName"]
                )
                await delete_method(
                    push_command.data_json.tenant,
                    command[1],
                    dimension_name,
                    hierarchy_name,
                    send_to_aor=False,
                    check_possible_delete=False,
                )
            else:
                await delete_method(
                    push_command.data_json.tenant,
                    command[1],
                    push_command.data_json.data_json["name"],
                    send_to_aor=False,
                    check_possible_delete=False,
                )
        elif command[0] == CommandEnum.COPY:
            logger.info(
                "Copy %s %s.%s to %s",
                push_command.type,
                push_command.data_json.tenant,
                push_command.data_json.data_json["name"],
                command[1],
            )
            copy_method = updaters[push_command.type][3]
            await copy_method(
                push_command.data_json.tenant,
                [command[1]],
                [push_command.data_json.data_json["name"]],
                send_to_aor=False,
                check_possible_delete=False,
                raise_if_error=True,
            )
        logger.info(
            "Command for %s %s.%s completed",
            push_command.type,
            push_command.data_json.tenant,
            push_command.data_json.data_json["name"],
        )

    async def deploy_by_aor(self, push_command: PushAorCommand) -> None:
        """
        Выполняет развертывание объектов (моделей, баз данных и пр.) на основе команды.

        Args:
            push_command (PushAorCommand): Объект команды развёртывания,
                                        содержащий необходимые параметры для выполнения операции.
        """
        object_name = push_command.data_json.data_json.get("name", push_command.name)
        logger.info(
            "Deploy object %s %s.%s %s",
            push_command.type,
            push_command.data_json.tenant,
            object_name,
            push_command.version,
        )
        self.pop_not_processed_fields(push_command.data_json.data_json)
        self.__apply_schema_override_to_payload(push_command)
        commands = await self.__get_commands_by_push_command(push_command)
        for command in commands:
            if push_command.type in {AorType.DATABASE, AorType.MODEL}:
                await self.__update_not_linked_to_model_objects(push_command, command)
                continue
            await self.__update_linked_to_model_objects(push_command, command)

    async def send_model_and_database_to_aor(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        await self.database_service.send_to_aor_by_name(
            tenant_id,
            model.database_name,
            deleted=False,
            custom_uuid=aor_model_request.space_id,
            with_parents=aor_model_request.with_parents,
            version_suffix=aor_model_request.version_suffix,
            parent_version_suffix=aor_model_request.parent_version_suffix,
            name_suffix=aor_model_request.name_suffix,
            parent_name_suffix=aor_model_request.parent_name_suffix,
        )
        await self.model_service.create_and_send_command_to_aor_by_model(
            tenant_id,
            model,
            deleted=False,
            custom_uuid=aor_model_request.space_id,
            with_parents=aor_model_request.with_parents,
            version_suffix=aor_model_request.version_suffix,
            parent_version_suffix=aor_model_request.parent_version_suffix,
            name_suffix=aor_model_request.name_suffix,
            parent_name_suffix=aor_model_request.parent_name_suffix,
        )

    async def send_dimensions_to_aor(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        dimensions = await self.dimension_service.get_dimension_list_by_model_name(tenant_id, model.name)
        if not dimensions:
            raise ValueError(f"Not found dimensions for tenant_id={tenant_id}, model_name={model.name}")
        if not aor_model_request.dim_with_attributes:
            dimensions = self.graph_service.get_topological_order_dimensions_by_ref_dimension(dimensions)
            for dimension in dimensions:
                dimension.attributes = []
                await self.dimension_service.create_and_send_command_to_aor_by_dimension(
                    tenant_id,
                    dimension,
                    custom_uuid=aor_model_request.space_id,
                    with_parents=aor_model_request.with_parents,
                    dim_with_attributes=aor_model_request.dim_with_attributes,
                    depends_no_attrs_versions=True,
                    version_suffix=aor_model_request.version_suffix,
                    name_suffix=aor_model_request.name_suffix,
                    parent_name_suffix=aor_model_request.parent_name_suffix,
                )
        else:
            for dimension in dimensions:
                await self.dimension_service.create_and_send_command_to_aor_by_dimension(
                    tenant_id,
                    dimension,
                    custom_uuid=aor_model_request.space_id,
                    with_parents=aor_model_request.with_parents,
                    dim_with_attributes=aor_model_request.dim_with_attributes,
                    depends_no_attrs_versions=aor_model_request.depends_no_attrs_versions,
                    version_suffix=aor_model_request.version_suffix,
                    parent_version_suffix=aor_model_request.parent_version_suffix,
                    name_suffix=aor_model_request.name_suffix,
                    parent_name_suffix=aor_model_request.parent_name_suffix,
                )
        logger.info("%s dimensions sended", len(dimensions))

    async def send_measures_to_aor(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        measures = await self.measure_service.get_measure_list_by_model_name(tenant_id, model.name)
        if not measures:
            raise ValueError(f"Not found measures for tenant_id={tenant_id}, model_name={model.name}")
        for measure in measures:
            await self.measure_service.create_and_send_command_to_aor_by_measure(
                tenant_id,
                measure,
                custom_uuid=aor_model_request.space_id,
                with_parents=aor_model_request.with_parents,
                depends_no_attrs_versions=aor_model_request.depends_no_attrs_versions,
                version_suffix=aor_model_request.version_suffix,
                parent_version_suffix=aor_model_request.parent_version_suffix,
                name_suffix=aor_model_request.name_suffix,
                parent_name_suffix=aor_model_request.parent_name_suffix,
            )
        logger.info("%s measures sended", len(measures))

    async def send_datastorage_to_model(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        datastorages = await self.datastorage_service.get_data_storage_list_by_model_name(tenant_id, model.name)
        if not datastorages:
            raise ValueError(f"Not found datastorages for tenant_id={tenant_id}, model_name={model.name}")
        count_ds = 0
        for datastorage in datastorages:
            if datastorage.type in {
                DataStorageEnum.DIMENSION_TEXTS,
                DataStorageEnum.DIMENSION_ATTRIBUTES,
                DataStorageEnum.DIMENSION_VALUES,
                DataStorageEnum.HIERARCHY_NODES,
                DataStorageEnum.HIERARCHY_TEXTNODES,
                DataStorageEnum.HIERARCHY_TEXTVERSIONS,
                DataStorageEnum.HIERARCHY_VERSIONS,
            }:
                continue
            await self.datastorage_service.create_and_send_command_to_aor_by_datastorage(
                tenant_id,
                datastorage,
                custom_uuid=aor_model_request.space_id,
                with_parents=aor_model_request.with_parents,
                depends_no_attrs_versions=aor_model_request.depends_no_attrs_versions,
                version_suffix=aor_model_request.version_suffix,
                parent_version_suffix=aor_model_request.parent_version_suffix,
                name_suffix=aor_model_request.name_suffix,
                parent_name_suffix=aor_model_request.parent_name_suffix,
            )
            count_ds += 1
        logger.info("%s datastorages sended", count_ds)

    async def send_composite_to_model(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        composites = await self.compositre_service.get_composite_list_by_model_name(tenant_id, model.name)
        if not composites:
            raise ValueError(f"Not found composites for tenant_id={tenant_id}, model_name={model.name}")
        composites = self.graph_service.get_topological_order_composites(composites)
        for composite in composites:
            await self.compositre_service.create_and_send_command_to_aor_by_composite(
                tenant_id,
                composite,
                custom_uuid=aor_model_request.space_id,
                with_parents=aor_model_request.with_parents,
                depends_no_attrs_versions=aor_model_request.depends_no_attrs_versions,
                version_suffix=aor_model_request.version_suffix,
                parent_version_suffix=aor_model_request.parent_version_suffix,
                name_suffix=aor_model_request.name_suffix,
                parent_name_suffix=aor_model_request.parent_name_suffix,
            )
        logger.info("%s composites sended", len(composites))

    async def send_hierarchy_to_aor(
        self, tenant_id: str, model: Model, aor_model_request: CreateModelAorRequest
    ) -> None:
        hierarchies = await self.hierarchy_service.get_multi(tenant_id, model.name, [], [])
        if not hierarchies:
            raise ValueError(f"Not found hierarchies for tenant_id={tenant_id}, model_name={model.name}")
        for hierarchy in hierarchies:
            await self.hierarchy_service.create_and_send_command_to_aor_by_hierarchy(
                tenant_id,
                hierarchy,
                custom_uuid=aor_model_request.space_id,
                with_parents=aor_model_request.with_parents,
                depends_no_attrs_versions=aor_model_request.depends_no_attrs_versions,
                version_suffix=aor_model_request.version_suffix,
                parent_version_suffix=aor_model_request.parent_version_suffix,
                name_suffix=aor_model_request.name_suffix,
                parent_name_suffix=aor_model_request.parent_name_suffix,
            )
        logger.info("%s hierarchy sended", len(hierarchy))

    async def send_to_aor_by_model_and_type(
        self, tenant_id: str, model_name: str, aor_model_request: CreateModelAorRequest
    ) -> None:
        """
        Отправляет запрос на создание объекта в AOR.

        Args:
            tenant_id (str): Идентификатор арендатора/организации.
            aor_request (CreateAorRequest): Запрос на создание нового объекта AOR, содержащий следующие поля:
                type (str): Тип создаваемого объекта AOR.
                name (str): Имя создаваемого объекта AOR.
                is_deleted (bool): Флаг удаления объекта AOR (если true — объект считается удалённым).
                space_id (UUID): Уникальный идентификатор пространства в AOR.
                with_parents (bool): Флаг отправки родительских объктов.

        """
        send_to_aor_map = {
            AORCreateRequestTypesEnum.MODEL: self.send_model_and_database_to_aor,
            AORCreateRequestTypesEnum.DIMENSION: self.send_dimensions_to_aor,
            AORCreateRequestTypesEnum.MEASURE: self.send_measures_to_aor,
            AORCreateRequestTypesEnum.DATASTORAGE: self.send_datastorage_to_model,
            AORCreateRequestTypesEnum.COMPOSITE: self.send_composite_to_model,
            AORCreateRequestTypesEnum.HIERARCHY: self.send_hierarchy_to_aor,
            AORCreateRequestTypesEnum.DATABASEOBJECT: self.datastorage_service.send_database_objects_to_model,
        }
        model = await self.model_service.get_model_by_name_or_null(tenant_id, model_name)
        if model is None:
            raise ValueError(f"Model with tenant_id={tenant_id}, name={model_name} not found")
        await send_to_aor_map[aor_model_request.type](tenant_id, model, aor_model_request)
        return None
