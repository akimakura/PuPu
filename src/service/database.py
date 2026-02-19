"""
Сервис баз данных.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from typing import Optional
from uuid import UUID

from py_common_lib.logger import EPMPYLogger
from py_common_lib.starlette_context_plugins import AuthorizationPlugin
from sqlalchemy.exc import NoResultFound
from starlette_context import context

from src.integration.aor.client import ClientAOR
from src.integration.aor.model import AorType, CreateAorCommand, JsonData
from src.models.database import Database, DatabaseCreateRequest, DatabaseEditRequest
from src.models.request_params import Pagination
from src.repository.cache import CacheRepository
from src.repository.database import DatabaseRepository
from src.service.utils import get_updated_fields_object
from src.utils.auth import get_user_login_by_token
from src.utils.backoff import RetryConfig, retry

logger = EPMPYLogger(__name__)


class DatabaseService:
    def __init__(self, data_repository: DatabaseRepository, aor_client: ClientAOR) -> None:
        self.data_repository: DatabaseRepository = data_repository
        self.aor_client = aor_client

    @retry(RetryConfig())
    async def get_database_by_name(self, tenant_id: str, name: str) -> Database:
        """Получить базу данных по имени."""
        result = await self.data_repository.get_by_name(tenant_id=tenant_id, name=name)
        return result

    async def get_database_by_name_or_null(self, tenant_id: str, name: str) -> Optional[Database]:
        """Получить базу данных по имени."""
        try:
            return await self.get_database_by_name(tenant_id=tenant_id, name=name)
        except NoResultFound:
            return None

    @retry(RetryConfig())
    async def get_database_list(self, tenant_id: str, pagination: Optional[Pagination] = None) -> list[Database]:
        """Получить список всех баз данных."""
        result = await self.data_repository.get_list(
            tenant_id=tenant_id,
            pagination=pagination,
        )
        return result

    async def delete_database_by_name(self, tenant_id: str, database_name: str, send_to_aor: bool = True) -> None:
        """Удалить базу данных."""
        database = await self.get_database_by_name(tenant_id=tenant_id, name=database_name)
        command = await self.create_and_send_command_to_aor_by_database(
            tenant_id=tenant_id, database=database, deleted=True, send_command=False
        )
        await self.data_repository.delete_by_name(tenant_id=tenant_id, name=database_name)
        await CacheRepository.clear_databases_cache(tenant_id=tenant_id)
        await CacheRepository.clear_database_cache_by_name(tenant_id=tenant_id, name=database_name)
        if send_to_aor:
            await self.aor_client.send_request(command)
        return None

    async def create_database_by_schema(
        self, tenant_id: str, database: DatabaseCreateRequest, send_to_aor: bool = True
    ) -> Database:
        """Создать базу данных."""
        result = await self.data_repository.create_by_schema(tenant_id=tenant_id, database=database)
        await CacheRepository.clear_databases_cache(tenant_id=tenant_id)
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_database(database=result, tenant_id=tenant_id)
        return result

    async def update_database_by_name_and_schema(
        self, tenant_id: str, database_name: str, database: DatabaseEditRequest, send_to_aor: bool = True
    ) -> Database:
        """Обновить базу данных."""
        result = await self.data_repository.update_by_name_and_schema(
            tenant_id=tenant_id, name=database_name, database=database
        )
        if send_to_aor:
            await self.create_and_send_command_to_aor_by_database(tenant_id=tenant_id, database=result)
        await CacheRepository.clear_databases_cache(tenant_id=tenant_id)
        await CacheRepository.clear_database_cache_by_name(tenant_id=tenant_id, name=database_name)
        return result

    async def get_updated_fields(self, tenant_id: str, name: str, database: DatabaseEditRequest) -> dict:
        """Получить поля, которые были изменены"""
        original_database = await self.get_database_by_name(tenant_id=tenant_id, name=name)
        original_database = original_database.model_dump(mode="json", by_alias=True)
        database_dict = database.model_dump(mode="json", by_alias=True, exclude_unset=True)
        return get_updated_fields_object(original_database, database_dict)

    async def create_and_send_command_to_aor_by_database(
        self,
        tenant_id: str,
        database: Database,
        deleted: bool = False,
        custom_uuid: Optional[UUID] = None,
        send_command: bool = True,
        version_suffix: str = "",
        parent_version_suffix: str = "",
        name_suffix: str = "",
        parent_name_suffix: str = "",
    ) -> Optional[CreateAorCommand]:
        command_model = None
        data_json = JsonData(
            is_deleted=deleted, tenant=tenant_id, data_json=database.model_dump(mode="json", by_alias=True)
        )
        command = {
            "type": AorType.DATABASE,
            "name": database.name + name_suffix,
            "data_json": data_json,
            "description": database.name,
            "version": (
                str(database.version) + version_suffix
                if not deleted
                else f"{database.version}-deleted" + version_suffix
            ),
            "external_object_id": database.name + name_suffix,
            "deployed_by": get_user_login_by_token(context.get(AuthorizationPlugin.key) or None),
        }
        if custom_uuid:
            command["space_id"] = custom_uuid
        try:
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
        database = await self.get_database_by_name(tenant_id, name)
        await self.create_and_send_command_to_aor_by_database(
            tenant_id=tenant_id,
            database=database,
            deleted=deleted,
            custom_uuid=custom_uuid,
            version_suffix=version_suffix,
            parent_version_suffix=parent_version_suffix,
            name_suffix=name_suffix,
            parent_name_suffix=parent_name_suffix,
        )

    def __repr__(self) -> str:
        return "DatabaseService"
