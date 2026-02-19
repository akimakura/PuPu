"""
Репозиторий таблиц.
"""

from collections.abc import Sequence
from typing import Optional

from py_common_lib.utils import timeit
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database_object import DatabaseObject, DatabaseObjectModelRelation, DataBaseObjectSpecificAttribute
from src.db.model import Model
from src.models.database_object import (
    DatabaseObject as DatabaseObjectModel,
    DataBaseObjectSpecificAttribute as DataBaseObjectSpecificAttributeModel,
    DbObjectTypeEnum,
)
from src.models.model import Model as ModelModel
from src.repository.history.database_object import DatabaseObjectHistoryRepository


class DatabaseObjectRepository:

    def __init__(
        self,
        session: AsyncSession,
    ) -> None:
        self.session = session
        self.history_repository = DatabaseObjectHistoryRepository(session)

    def _convert_specefic_attributes_list_to_orm(
        self, attributes: Optional[list[DataBaseObjectSpecificAttributeModel]]
    ) -> Optional[list[DataBaseObjectSpecificAttribute]]:
        """Конвертация list[DataBaseObjectSpecificAttributeModel] -> list[DataBaseObjectSpecificAttributeORM]"""
        if attributes is None:
            return []
        return [DataBaseObjectSpecificAttribute(**(attr.model_dump(mode="json"))) for attr in attributes]

    async def _get_orm_database_objects_by_models(
        self,
        tenant_id: str,
        database_objects: list[DatabaseObjectModel],
    ) -> Sequence[DatabaseObject]:
        """Получить объекты базы данных по моделям."""
        filters = []
        for db_object in database_objects:
            filters.append(
                (DatabaseObject.name == db_object.name)
                & (DatabaseObject.schema_name == db_object.schema_name)
                & (DatabaseObject.tenant_id == tenant_id)
            )
        result = (await self.session.execute(select(DatabaseObject).filter(or_(*filters)))).scalars().all()
        return result

    async def get_by_ids(self, ids: Sequence[int], tenant_id: Optional[str] = None) -> list[DatabaseObject]:
        """Получить список database_object по id с опциональной фильтрацией по tenant_id."""
        if not ids:
            return []
        query = select(DatabaseObject).where(DatabaseObject.id.in_(ids))
        if tenant_id is not None:
            query = query.where(DatabaseObject.tenant_id == tenant_id)
        return list((await self.session.execute(query)).scalars().all())

    @timeit
    async def create_orm_db_objects(
        self,
        tenant_id: str,
        db_objects: list[DatabaseObjectModel],
        models: list[Model],
        data_storage_id: Optional[int] = None,
        composite_id: Optional[int] = None,
    ) -> list[DatabaseObject]:
        """Создать объекты databaseObject в бд."""
        model_models = [ModelModel.model_validate(model) for model in models]
        result = []
        for db_object in db_objects:
            db_object_dict = db_object.model_dump(mode="json")
            attributes = self._convert_specefic_attributes_list_to_orm(db_object.specific_attributes)
            db_object_dict["specific_attributes"] = attributes
            db_object_dict["tenant_id"] = tenant_id
            if not db_object_dict.get("schema_name"):
                db_object_dict["schema_name"] = model_models[0].schema_name
            if data_storage_id:
                db_object_dict["data_storage_id"] = data_storage_id
            if composite_id:
                db_object_dict["composite_id"] = composite_id
            created_db_object = DatabaseObject(**db_object_dict)
            created_db_object.models = models
            self.session.add(created_db_object)
            result.append(created_db_object)
        return result

    async def get_view_by_identity(self, tenant_id: str, schema_name: str, name: str) -> Optional[DatabaseObject]:
        """Возвращает VIEW по ключу (tenant_id, schema_name, name)."""
        query = select(DatabaseObject).where(
            DatabaseObject.tenant_id == tenant_id,
            DatabaseObject.schema_name == schema_name,
            DatabaseObject.name == name,
            DatabaseObject.type == DbObjectTypeEnum.VIEW,
        )
        return (await self.session.execute(query)).scalars().one_or_none()

    async def get_view_by_name(
        self,
        tenant_id: str,
        name: str,
        model_name: Optional[str] = None,
    ) -> Optional[DatabaseObject]:
        """Получить VIEW по имени"""
        query = select(DatabaseObject).where(
            DatabaseObject.tenant_id == tenant_id,
            DatabaseObject.name == name,
            DatabaseObject.type == DbObjectTypeEnum.VIEW,
        )
        if model_name is not None:
            query = (
                query.distinct()
                .join(DatabaseObjectModelRelation, DatabaseObjectModelRelation.database_object_id == DatabaseObject.id)
                .join(Model, DatabaseObjectModelRelation.model_id == Model.id)
                .where(Model.name == model_name, Model.tenant_id == tenant_id)
            )
        results = list((await self.session.execute(query)).scalars().all())
        if not results:
            return None
        if len(results) > 1:
            raise ValueError(f"Found multiple VIEW objects with name={name} for tenant_id={tenant_id}.")
        return results[0]

    async def get_views_by_model(self, tenant_id: str, model_name: str) -> list[DatabaseObject]:
        """Получить все VIEW, связанные с моделью."""
        query = (
            select(DatabaseObject)
            .join(DatabaseObjectModelRelation, DatabaseObjectModelRelation.database_object_id == DatabaseObject.id)
            .join(Model, DatabaseObjectModelRelation.model_id == Model.id)
            .where(
                DatabaseObject.tenant_id == tenant_id,
                DatabaseObject.type == DbObjectTypeEnum.VIEW,
                Model.name == model_name,
            )
        )
        return list((await self.session.execute(query)).scalars().all())

    async def upsert_view(
        self,
        tenant_id: str,
        schema_name: str,
        name: str,
        json_definition: dict,
        models: list[Model],
    ) -> Optional[DatabaseObject]:
        """Создаёт или обновляет VIEW и возвращает ORM-объект."""
        existing = await self.get_view_by_identity(tenant_id, schema_name, name)
        if existing is not None and existing.composite_id is not None:
            return None
        if existing is not None:
            if existing.json_definition == json_definition:
                self._ensure_models(existing, models)
                return existing
            await self.history_repository.save_history(existing, new_definition=json_definition)
            existing.json_definition = json_definition
            self._ensure_models(existing, models)
            await self._update_model_relations_version(existing)
            return existing
        created = DatabaseObject(
            name=name,
            schema_name=schema_name,
            type=DbObjectTypeEnum.VIEW,
            tenant_id=tenant_id,
            json_definition=json_definition,
        )
        await self.history_repository.update_version(created, create=True)
        created.models = models
        self.session.add(created)
        await self.session.flush()
        return created

    @staticmethod
    def _ensure_models(db_object: DatabaseObject, models: list[Model]) -> None:
        """Добавляет недостающие связи с моделями для объекта."""
        if not models:
            return
        existing_ids = {model.id for model in db_object.models}
        for model in models:
            if model.id not in existing_ids:
                db_object.models.append(model)

    async def _update_model_relations_version(self, db_object: DatabaseObject) -> None:
        """Обновляет версию связей database_object_model_relation до версии объекта."""
        relations = (
            (
                await self.session.execute(
                    select(DatabaseObjectModelRelation).where(
                        DatabaseObjectModelRelation.database_object_id == db_object.id
                    )
                )
            )
            .scalars()
            .all()
        )
        self.history_repository.update_obj_version(relations, db_object.timestamp, db_object.user, db_object.version)
