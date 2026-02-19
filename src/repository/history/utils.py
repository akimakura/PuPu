from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database_object import DatabaseObject, DatabaseObjectModelRelation


async def get_database_object_relations(
    session: AsyncSession, database_objects: list[DatabaseObject]
) -> Sequence[DatabaseObjectModelRelation]:
    """
    Получает все связи объектов базы данных с моделями.

    Функция извлекает идентификаторы из списка `database_objects`,
    затем выполняет асинхронный запрос к базе данных для получения всех записей
    типа `DatabaseObjectModelRelation`, где `database_object_id` совпадает
    с одним из идентификаторов из входного списка.

    Args:
        session (AsyncSession): Асинхронная сессия SQLAlchemy для выполнения запроса.
        database_objects (list[DatabaseObject]): Список объектов базы данных,
            для которых нужно найти связи.

    Returns:
        Sequence[DatabaseObjectModelRelation]: Последовательность найденных связей.
            Если совпадений нет, возвращается пустая последовательность.
    """
    db_object_ids = (database_object.id for database_object in database_objects)
    return (
        (
            await session.execute(
                select(DatabaseObjectModelRelation).where(
                    DatabaseObjectModelRelation.database_object_id.in_(db_object_ids)
                )
            )
        )
        .scalars()
        .all()
    )
