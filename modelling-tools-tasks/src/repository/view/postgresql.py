from typing import Optional

from src.db.engines.postgresql.engine import execute_raw_DDL
from src.repository.view.core import ViewRepository


class ViewPostgreSQLRepository(ViewRepository):
    async def _execute_DDL(self, queries: str | list[str]) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        return await execute_raw_DDL(self.tenant_id, self.database, queries)

    def _get_delete_view_sql(self, schema_name: str, name: str, cluster_name: Optional[str] = None) -> str:
        """Генерация DDL, который удаляет VIEW."""
        return f"DROP VIEW IF EXISTS {schema_name}.{name}"

    def _get_create_view_sql(
        self,
        schema_name: str,
        name: str,
        sql_expression: str,
        cluster_name: Optional[str] = None,
        replace: bool = False,
    ) -> list[str]:
        """Генерация DDL, который создает или пересоздает VIEW."""
        result = []
        if replace:
            drop_view_sql = self._get_delete_view_sql(schema_name=schema_name, name=name, cluster_name=cluster_name)
            result.append(drop_view_sql)
        create_view_sql = f"CREATE VIEW {schema_name}.{name} AS ({sql_expression})"
        result.append(create_view_sql)
        return result
