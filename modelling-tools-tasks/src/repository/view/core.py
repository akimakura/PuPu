from typing import Optional

from py_common_lib.logger import EPMPYLogger

from src.integrations.modelling_tools_api.codegen import (
    CompositeGet as Composite,
    Database,
    DbObject,
)
from src.models.composite import CompositeFieldRefObjectEnum
from src.models.database_object import DatabaseObjectGenerationResult
from src.utils.text_exceptions import DB_OBJECTS_IS_NULL
from src.utils.validators import get_not_null_or_raise, is_enable_generate_objects

logger = EPMPYLogger(__name__)


class ViewRepository:
    def __init__(self, tenant_id: str, database: Database) -> None:
        self.tenant_id = tenant_id
        self.database = database

    def is_scenario_composite(self, composite: Composite) -> bool:
        return any([ds.type == CompositeFieldRefObjectEnum.CE_SCENARIO for ds in composite.datasources])

    async def _execute_DDL(self, queries: str | list[str]) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        raise NotImplementedError

    def _get_create_view_sql(
        self,
        schema_name: str,
        name: str,
        sql_expression: str,
        cluster_name: Optional[str] = None,
        replace: bool = False,
    ) -> list[str]:
        """Генерация DDL, который создает или пересоздает VIEW."""
        raise NotImplementedError

    def _get_delete_view_sql(self, schema_name: str, name: str, cluster_name: Optional[str] = None) -> str:
        """Генерация DDL, который удаляет VIEW."""
        raise NotImplementedError

    async def create_view_by_composite(
        self, composite: Composite, sql_expression: str, replace: bool = False
    ) -> list[DatabaseObjectGenerationResult]:
        """Создание представления (View) для композита в базе данных.

        Метод проверяет корректность входных данных, формирует SQL-запрос для создания
        или замены представления и выполняет его в указанной базе данных.

        Логика работы:
        1. Проверяет наличие источников данных типа CE_SCENARIO в композите.
        2. Учитывает глобальный флаг ENABLE_GENERATE_OBJECTS.
        3. Ищет объект базы данных, связанный с указанной моделью.
        4. Формирует SQL-запрос для создания/замены представления.
        5. Выполняет DDL-операцию в базе данных.
        6. Логгирует результаты выполнения операции.

        Args:
            composite (Composite): Объект композита, содержащий конфигурацию представления.
            model (Model): Модель, для которой создается представление.
            sql_expression (str): SQL-запрос, определяющий логику представления.
            replace (bool, optional): Флаг, указывающий на необходимость замены
                существующего представления. Defaults to False.

        Returns:
            Optional[str]: Сформированный SQL-запрос для создания представления,
                если операция выполнена успешно. None, если создание невозможно.

        Raises:
            ValueError: Если для модели не найден соответствующий объект базы данных
                в составе композита.


        """
        result: list[DatabaseObjectGenerationResult] = []
        if not is_enable_generate_objects() or self.is_scenario_composite(composite):
            return result
        db_objects: list[DbObject] = get_not_null_or_raise(composite.db_objects, custom_raise_text=DB_OBJECTS_IS_NULL)
        cluster_name = self.database.default_cluster_name
        view_name: str = get_not_null_or_raise(
            db_objects[0].name, log_attr_name="name", log_obj_name=str(db_objects[0])
        )
        schema_name: str = get_not_null_or_raise(
            db_objects[0].schema_name, log_attr_name="schema_name", log_obj_name=db_objects[0].name
        )
        sql_expression_create = self._get_create_view_sql(schema_name, view_name, sql_expression, cluster_name, replace)
        logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expression_create)
        try:
            result.append(
                DatabaseObjectGenerationResult(
                    table=db_objects[0],
                    sql_expression=" ".join(sql_expression_create),
                    executed=True,
                )
            )
            await self._execute_DDL(sql_expression_create)
        except Exception as exc:
            logger.exception("Error creating view '%s' in database '%s'", view_name, self.database.name)
            result[-1].error = str(exc)
        else:
            logger.debug(
                "View for composite with name=%s, schema_name=%s created in database.",
                composite.name,
                composite.schema_name,
            )
        return result

    async def update_view_by_composite(
        self, composite: Composite, sql_expression: str
    ) -> list[DatabaseObjectGenerationResult]:
        """
        Пересоздаёт представление (View) для композита в базе данных.

        Проверяет источники данных композита на наличие типа CE_SCENARIO,
        пропускает выполнение при отключённой генерации объектов (ENABLE_GENERATE_OBJECTS).
        Для каждого объекта из фильтрованного списка формирует SQL-запрос на обновление View
        и выполняет его через DDL-команду.

        Args:
            composite (Composite): Объект композита, содержащий источники данных.
            model (Model): Модель с данными о базе данных.
            sql_expression (str): SQL-выражение для создания представления.

        Returns:
            Optional[list[str]]: Возвращает последний сформированный SQL-запрос или None.

        Raises:
            ValueError: Если объект базы данных не содержит schema_name.
        """
        result: list[DatabaseObjectGenerationResult] = []
        if not is_enable_generate_objects() or self.is_scenario_composite(composite):
            return result
        database_objects: list[DbObject] = get_not_null_or_raise(
            composite.db_objects, custom_raise_text=DB_OBJECTS_IS_NULL
        )
        cluster_name = self.database.default_cluster_name
        database_object = database_objects[0]
        sql_expression_update = None
        schema_name: str = get_not_null_or_raise(
            database_object.schema_name, log_attr_name="schema_name", log_obj_name=database_object.name
        )
        sql_expression_update = self._get_create_view_sql(
            schema_name, database_object.name, sql_expression, cluster_name, replace=True
        )
        logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expression_update)
        try:
            result.append(
                DatabaseObjectGenerationResult(
                    table=database_objects[0],
                    sql_expression=" ".join(sql_expression_update),
                    executed=True,
                )
            )
            await self._execute_DDL(sql_expression_update)
        except Exception as exc:
            logger.exception("Error updating view '%s' in database '%s'", database_object.name, self.database.name)
            result[-1].error = str(exc)
        else:
            logger.debug(
                "View for composite with name=%s, schema_name=%s updated in database.",
                database_object.name,
                database_object.schema_name,
            )
        return result

    async def delete_view_by_composite(
        self,
        composite: Composite,
    ) -> Optional[str]:
        """
        Удаление View для композита в базе данных.

        Метод проверяет источники данных композита и выполняет удаление связанных представлений (views),
        если это разрешено настройками. Возвращает последний выполненный SQL-запрос или None.

        Args:
            composite (Composite): Объект композита, содержащий источники данных.
            model (Model): Модель базы данных, связанная с композитом.
            database_objects (Optional[list[DatabaseObjectModel]]): Список объектов базы данных для удаления.
                Если не указан, будет выполнен фильтр по хранилищу данных композита.

        Returns:
            Optional[str]: SQL-запрос, использованный для удаления последнего view, или None, если операция не выполнена.

        Raises:
            ValueError: Если объект базы данных не содержит схему (schema_name).

        """
        if not is_enable_generate_objects() or self.is_scenario_composite(composite):
            return None
        database_objects: list[DbObject] = get_not_null_or_raise(
            composite.db_objects, custom_raise_text=DB_OBJECTS_IS_NULL
        )
        cluster_name = self.database.default_cluster_name
        sql_expression = None
        for database_object in database_objects:
            schema_name: str = get_not_null_or_raise(
                database_object.schema_name, log_attr_name="schema_name", log_obj_name=database_object.name
            )
            sql_expression = self._get_delete_view_sql(schema_name, database_object.name, cluster_name)
            logger.debug("SQL_EXPRESSION created. sql_expression='%s'", sql_expression)
            await self._execute_DDL(sql_expression)
            logger.debug(
                "View for composite with name=%s, schema_name=%s deleted from database.",
                database_object.name,
                database_object.schema_name,
            )
        return sql_expression
