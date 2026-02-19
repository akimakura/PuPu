from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.data_storage import DataStorage
from src.db.database_object import DatabaseObject, DatabaseObjectRelation
from src.models.database_object import DatabaseObjectRelationTypeEnum, DbObjectTypeEnum
from src.models.tenant import SemanticObjectsTypeEnum


class DatabaseObjectRelationsRepository:
    """Репозиторий для работы с таблицей database_object_relations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_relation(
        self,
        semantic_object_type: SemanticObjectsTypeEnum,
        semantic_object_id: int,
        semantic_object_version: int,
        database_object_id: int,
        database_object_version: int,
        relation_type: DatabaseObjectRelationTypeEnum,
    ) -> Optional[DatabaseObjectRelation]:
        """Возвращает связь, если она уже существует."""
        query = select(DatabaseObjectRelation).where(
            DatabaseObjectRelation.semantic_object_type == semantic_object_type,
            DatabaseObjectRelation.semantic_object_id == semantic_object_id,
            DatabaseObjectRelation.semantic_object_version == semantic_object_version,
            DatabaseObjectRelation.database_object_id == database_object_id,
            DatabaseObjectRelation.database_object_version == database_object_version,
            DatabaseObjectRelation.relation_type == relation_type,
        )
        return (await self.session.execute(query)).scalars().one_or_none()

    async def get_next_version(
        self, semantic_object_type: SemanticObjectsTypeEnum, semantic_object_id: int, database_object_id: int
    ) -> int:
        """Возвращает следующую версию для новой связи."""
        query = select(func.max(DatabaseObjectRelation.version)).where(
            DatabaseObjectRelation.semantic_object_type == semantic_object_type,
            DatabaseObjectRelation.semantic_object_id == semantic_object_id,
            DatabaseObjectRelation.database_object_id == database_object_id,
        )
        result = await self.session.execute(query)
        current = result.scalar()
        return (current or 0) + 1

    async def ensure_relation(
        self,
        semantic_object_type: SemanticObjectsTypeEnum,
        semantic_object_id: int,
        semantic_object_version: int,
        database_object_id: int,
        database_object_version: int,
        relation_type: DatabaseObjectRelationTypeEnum = DatabaseObjectRelationTypeEnum.PARENT,
    ) -> DatabaseObjectRelation:
        """Создаёт связь, если её ещё нет, и возвращает объект связи."""
        existing = await self.get_relation(
            semantic_object_type,
            semantic_object_id,
            semantic_object_version,
            database_object_id,
            database_object_version,
            relation_type,
        )
        if existing is not None:
            return existing
        version = await self.get_next_version(semantic_object_type, semantic_object_id, database_object_id)
        relation = DatabaseObjectRelation(
            semantic_object_type=semantic_object_type,
            semantic_object_id=semantic_object_id,
            semantic_object_version=semantic_object_version,
            database_object_id=database_object_id,
            database_object_version=database_object_version,
            relation_type=relation_type,
            version=version,
        )
        self.session.add(relation)
        await self.session.flush()
        return relation

    async def get_datastorage_parents_by_database_object(
        self, tenant_id: str, database_object_id: int, database_object_version: int
    ) -> list[tuple[str, int]]:
        """Возвращает список родительских DATA_STORAGE для указанного database_object."""
        query = (
            select(DataStorage.name, DatabaseObjectRelation.semantic_object_version)
            .select_from(DatabaseObjectRelation)
            .join(DataStorage, DataStorage.id == DatabaseObjectRelation.semantic_object_id)
            .where(
                DatabaseObjectRelation.semantic_object_type == SemanticObjectsTypeEnum.DATA_STORAGE,
                DatabaseObjectRelation.relation_type == DatabaseObjectRelationTypeEnum.PARENT,
                DatabaseObjectRelation.database_object_id == database_object_id,
                DatabaseObjectRelation.database_object_version == database_object_version,
                DataStorage.tenant_id == tenant_id,
            )
        )
        return list((await self.session.execute(query)).tuples().all())

    async def get_views_by_datastorage(
        self,
        tenant_id: str,
        data_storage_id: int,
        data_storage_version: Optional[int] = None,
        relation_type: DatabaseObjectRelationTypeEnum = DatabaseObjectRelationTypeEnum.PARENT,
    ) -> list[DatabaseObject]:
        """Возвращает список VIEW, зависящих от указанного хранилища данных."""
        query = (
            select(DatabaseObject)
            .distinct()
            .join(DatabaseObjectRelation, DatabaseObjectRelation.database_object_id == DatabaseObject.id)
            .where(
                DatabaseObjectRelation.semantic_object_type == SemanticObjectsTypeEnum.DATA_STORAGE,
                DatabaseObjectRelation.semantic_object_id == data_storage_id,
                DatabaseObjectRelation.relation_type == relation_type,
                DatabaseObject.tenant_id == tenant_id,
                DatabaseObject.type == DbObjectTypeEnum.VIEW,
            )
        )
        if data_storage_version is not None:
            query = query.where(DatabaseObjectRelation.semantic_object_version == data_storage_version)
        return list((await self.session.execute(query)).scalars().all())
