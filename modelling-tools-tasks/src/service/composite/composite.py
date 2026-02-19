from py_common_lib.logger import EPMPYLogger

from src.config import settings
from src.integrations.modelling_tools_api.codegen import (
    CompositeGet as Composite,
    DbObject,
    InternalApi,
    ObjectStatus,
    ObjectType,
    RespObjectStatus,
    Result,
    V1Api,
)
from src.models.database_object import DatabaseObjectGenerationResult
from src.repository.view.core import ViewRepository
from src.service.composite.graph import GraphService
from src.service.composite.sql_generator import CompositeSQLGenerator
from src.service.utils import change_model_status, get_change_object_status
from src.utils.validators import get_not_null_or_raise

logger = EPMPYLogger(__name__)


class CompositeService:
    def __init__(
        self,
        mt_api_v1_client: V1Api,
        mt_internal_api_client: InternalApi,
        model_name: str | None = None,
        tenant_id: str | None = None,
        sql_generator: CompositeSQLGenerator | None = None,
    ) -> None:
        self.mt_api_v1_client = mt_api_v1_client
        self.mt_internal_api_client = mt_internal_api_client
        self._view_repository: ViewRepository | None = None
        self._model_name: str | None = model_name
        self._tenant_id: str | None = tenant_id
        self._sql_generator: CompositeSQLGenerator | None = sql_generator
        self.graph_service: GraphService = GraphService()

    @property
    def view_repository(self) -> ViewRepository:
        return get_not_null_or_raise(self._view_repository, log_attr_name="_view_repository")

    @view_repository.setter
    def view_repository(self, repository: ViewRepository) -> None:
        self._view_repository = repository

    @property
    def model_name(self) -> str:
        return get_not_null_or_raise(self._model_name, log_attr_name="_model_name")

    @model_name.setter
    def model_name(self, name: str) -> None:
        self._model_name = name

    @property
    def tenant_id(self) -> str:
        return get_not_null_or_raise(self._tenant_id, log_attr_name="_tenant_id")

    @tenant_id.setter
    def tenant_id(self, name: str) -> None:
        self._tenant_id = name

    @property
    def sql_generator(self) -> CompositeSQLGenerator:
        return get_not_null_or_raise(self._sql_generator, log_attr_name="_sql_generator")

    @sql_generator.setter
    def sql_generator(self, generator_sql: CompositeSQLGenerator) -> None:
        self._sql_generator = generator_sql

    async def get_composite(self, name: str) -> Composite:
        composite = await self.mt_api_v1_client.get_composite_by_model_name_and_composite_name(
            composite_name=name,
            model_name=self.model_name,
            tenant_name=self.tenant_id,
            _request_timeout=settings.MT_API_TIMEOUT,
        )
        return composite

    async def get_composites(self) -> list[Composite]:
        composites = await self.mt_api_v1_client.get_composites_by_model_name(
            model_name=self.model_name,
            tenant_name=self.tenant_id,
            _request_timeout=settings.MT_API_TIMEOUT,
        )
        return composites

    def get_change_composite_status(
        self, datastorage_name: str, change_result: list[DatabaseObjectGenerationResult]
    ) -> list[ObjectStatus]:
        return get_change_object_status(self.model_name, ObjectType.COMPOSITE, datastorage_name, change_result)

    async def update_composite_in_db(
        self,
        composite: Composite,
    ) -> list[ObjectStatus]:
        sql_expression = await self.sql_generator.generate_composite_sql_expression_by_composite(composite)
        db_objects: list[DbObject] = get_not_null_or_raise(
            composite.db_objects,
            log_attr_name="db_objects",
            log_obj_name=f"{self.tenant_id}/{self.model_name}/{composite.name}",
        )
        try:
            change_object_result = await self.view_repository.update_view_by_composite(composite, sql_expression)
        except Exception as exc:
            logger.exception("Error while updating composite")
            change_object_result = []
            for db_object in db_objects:
                change_object_result.append(DatabaseObjectGenerationResult(table=db_object, error=str(exc)))
        change_status_request = self.get_change_composite_status(composite.name, change_object_result)
        return change_status_request

    async def set_pending_status(self, composites: list[Composite]) -> list[RespObjectStatus]:
        result = []
        for composite in composites:
            db_objects: list[DbObject] = get_not_null_or_raise(composite.db_objects)
            for db_object in db_objects:
                result.append(
                    ObjectStatus.model_validate({
                        "schemaName": db_object.schema_name,
                        "objectName": db_object.name,
                        "modelName": self.model_name,
                        "objectType": ObjectType.DATABASE_OBJECT,
                        "status": Result.PENDING,
                        "msg": None,
                    })
                )
            result.append(
                ObjectStatus.model_validate({
                    "schemaName": None,
                    "objectName": composite.name,
                    "modelName": self.model_name,
                    "objectType": ObjectType.COMPOSITE,
                    "status": Result.PENDING,
                    "msg": None,
                })
            )
        response = await change_model_status(self.tenant_id, self.model_name, self.mt_internal_api_client, result)
        return response

    async def create_composite_in_db(
        self,
        composite: Composite,
        replace: bool = False,
    ) -> list[ObjectStatus]:
        sql_expression = await self.sql_generator.generate_composite_sql_expression_by_composite(composite)
        db_objects: list[DbObject] = get_not_null_or_raise(
            composite.db_objects,
            log_attr_name="db_objects",
            log_obj_name=f"{self.tenant_id}/{self.model_name}/{composite.name}",
        )
        try:
            change_object_result = await self.view_repository.create_view_by_composite(
                composite, sql_expression, replace
            )
        except Exception as exc:
            logger.exception("Error while creating composite")
            change_object_result = []
            for db_object in db_objects:
                change_object_result.append(DatabaseObjectGenerationResult(table=db_object, error=str(exc)))
        change_status_request = self.get_change_composite_status(composite.name, change_object_result)
        return change_status_request

    async def create_composite_by_name(
        self,
        composite_name: str,
        replace: bool = False,
    ) -> list[ObjectStatus]:
        composite = await self.get_composite(composite_name)
        composite_res = await self.create_composite_in_db(composite, replace)
        return composite_res

    async def update_composite_by_name(
        self,
        composite_name: str,
    ) -> list[ObjectStatus]:
        composite = await self.get_composite(composite_name)
        composite_res = await self.update_composite_in_db(composite)
        return composite_res

    async def create_composite_by_name_and_send_status(
        self,
        composite_name: str,
        replace: bool = False,
    ) -> list[RespObjectStatus]:
        statuses = await self.create_composite_by_name(composite_name, replace)
        resp = await change_model_status(self.tenant_id, self.model_name, self.mt_internal_api_client, statuses)
        return resp

    async def update_composite_by_name_and_send_status(
        self,
        composite_name: str,
    ) -> list[RespObjectStatus]:
        statuses = await self.update_composite_by_name(composite_name)
        resp = await change_model_status(self.tenant_id, self.model_name, self.mt_internal_api_client, statuses)
        return resp

    async def delete_composite_in_db(self, composites: list[Composite]) -> None:
        logger.info("Run delete composites")
        if not composites:
            logger.info("No composites to delete")
        oriented_composites = self.graph_service.get_topological_order_composites(composites)
        for composite_num, composite in enumerate(oriented_composites):
            logger.info(
                "Delete composite %s/%s: %s.%s",
                composite_num + 1,
                len(oriented_composites),
                self.model_name,
                composite.name,
            )
            await self.view_repository.delete_view_by_composite(composite)
            logger.info("Deleted composite: %s.%s", self.model_name, composite.name)
