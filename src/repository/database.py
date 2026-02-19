"""
Репозиторий баз данных.
"""

from typing import Optional

from py_common_lib.utils import timeit
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database import Connection, Database, DatabaseLabel, Port
from src.models.database import (
    Database as DatabaseModel,
    DatabaseCreateRequest as DatabaseCreateRequestModel,
    DatabaseEditRequest as DatabaseEditRequestModel,
)
from src.models.request_params import Pagination
from src.repository.history.database import DatabaseHistoryRepository
from src.repository.utils import (
    add_missing_labels,
    convert_labels_list_to_orm,
    get_select_query_with_offset_limit_order,
)


class DatabaseRepository:

    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self.session = session
        self.database_history_repository = DatabaseHistoryRepository(session)

    async def _get_database_orm_by_session(self, tenant_id: str, name: str) -> Optional[Database]:
        """Получить Базу данных"""
        result = (
            (
                await self.session.execute(
                    select(Database).where(
                        Database.name == name,
                        Database.tenant_id == tenant_id,
                    )
                )
            )
            .scalars()
            .one_or_none()
        )
        return result

    async def get_database_orm_by_session_with_error(self, tenant_id: str, database_name: str) -> Database:
        database = await self._get_database_orm_by_session(tenant_id=tenant_id, name=database_name)
        if database:
            return database
        raise NoResultFound(f"""Database with tenant_id={tenant_id} and name={database_name} not found.""")

    def _convert_connections_to_orm(self, connections: list[dict]) -> list[Connection]:
        """
        Преобразует список словарей с данными о соединениях в список ORM-объектов Connection.

        Args:
            connections (list[dict]): Входной список словарей с данными о соединениях.

        Returns:
            list[Connection]: Список ORM-объектов Connection с инициализированными портами.
        """
        result = []
        for connection in connections:
            ports = connection["ports"]
            ports_orm = []
            for port in ports:
                port_orm = Port(**port)
                ports_orm.append(port_orm)
            connection.pop("ports")
            connection_orm = Connection(**connection)
            connection_orm.ports = ports_orm
            result.append(connection_orm)
        return result

    @timeit
    async def get_list(self, tenant_id: str, pagination: Optional[Pagination] = None) -> list[DatabaseModel]:
        """Получить список всех моделей"""
        query = select(Database).where(Database.tenant_id == tenant_id)
        query = get_select_query_with_offset_limit_order(query, Database.name, pagination)
        result = (await self.session.execute(query)).scalars().all()
        return [DatabaseModel.model_validate(database) for database in result]

    @timeit
    async def get_by_name(self, tenant_id: str, name: str) -> DatabaseModel:
        """Получить модель по её имени."""
        result = await self._get_database_orm_by_session(tenant_id=tenant_id, name=name)
        if result is None:
            raise NoResultFound(f"Database with tenant_id={tenant_id} and name={name} not found.")
        return DatabaseModel.model_validate(result)

    @timeit
    async def delete_by_name(self, tenant_id: str, name: str) -> None:
        """Удалить базу данных по имени."""
        result = await self._get_database_orm_by_session(tenant_id=tenant_id, name=name)
        if result is None:
            raise NoResultFound(f"Database with tenant_id={tenant_id} and name={name} not found.")
        await self.database_history_repository.save_history(result, deleted=True)
        await self.session.delete(result)
        await self.session.commit()

    @timeit
    async def create_by_schema(self, tenant_id: str, database: DatabaseCreateRequestModel) -> DatabaseModel:
        """
        Создаёт новую базу данных с учётом идентификатора тенанта (tenant_id) и входной модели.

        Args:
            tenant_id (str): Идентификатор клиента/арендодателя
            database (DatabaseCreateRequestModel): Входная модель с данными о базе данных

        Returns:
            DatabaseModel: Модель созданной базы данных с полными данными из БД
        """
        database_dict = database.model_dump(mode="json")
        database_dict["tenant_id"] = tenant_id
        add_missing_labels(database_dict["labels"], database.name)
        database_dict["labels"] = convert_labels_list_to_orm(database_dict["labels"], DatabaseLabel)
        connections = database_dict.pop("connections")
        connections_list_orm = self._convert_connections_to_orm(connections)
        database_dict["connections"] = connections_list_orm
        database_orm = Database(**database_dict)
        self.session.add(database_orm)
        await self.session.flush()
        await self.database_history_repository.update_version(database_orm, create=True)
        await self.session.commit()
        return DatabaseModel.model_validate(database_orm)

    @timeit
    async def update_by_name_and_schema(
        self, tenant_id: str, name: str, database: DatabaseEditRequestModel
    ) -> DatabaseModel:
        """
        Обновляет существующую базу данных по указанному именю и идентификатору тенанта (tenant_id).

        Args:
            tenant_id (str): Идентификатор тенанта, связанный с базой данных.
            name (str): Имя базы данных, которую необходимо обновить.
            database (DatabaseEditRequestModel): Модель с данными для обновления базы данных.

        Returns:
            DatabaseModel: Обновленная модель базы данных после применения изменений.

        Raises:
            NoResultFound: Если база данных с указанным tenant_id и именем не найдена.
        """
        database_dict = database.model_dump(mode="json", exclude_none=True)
        original_database = await self._get_database_orm_by_session(tenant_id=tenant_id, name=name)
        if original_database is None:
            raise NoResultFound(f"Database with tenant_id={tenant_id} and name={name} not found.")
        await self.database_history_repository.save_history(original_database, edit_model=database_dict)
        if database_dict.get("connections"):
            connections = database_dict.pop("connections")
            connections_list_orm = self._convert_connections_to_orm(connections)
            original_database.connections = connections_list_orm
        if database_dict.get("labels") is not None:
            add_missing_labels(database_dict["labels"], name)
            original_database.labels = convert_labels_list_to_orm(
                database_dict.pop("labels"),
                DatabaseLabel,
            )
        if database_dict:
            for attribute_name, attribute_value in database_dict.items():
                setattr(original_database, attribute_name, attribute_value)
        await self.database_history_repository.update_version(original_database)
        await self.session.commit()
        return DatabaseModel.model_validate(original_database)
