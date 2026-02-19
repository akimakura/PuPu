from typing import Optional

from src.config import settings
from src.db.engines.clickhouse.engine import execute_raw_DDL
from src.repository.view.core import ViewRepository


class ViewClickhouseRepository(ViewRepository):
    async def _execute_DDL(self, queries: str | list[str]) -> None:
        """
        Выполнение DDL запроса.
        Например: "CREATE", "ALTER TABLE", "DROP TABLE" и т.д
        """
        return await execute_raw_DDL(self.tenant_id, self.database, queries)

    def _get_create_view_sql(
        self,
        schema_name: str,
        name: str,
        sql_expression: str,
        cluster_name: Optional[str] = None,
        replace: bool = False,
    ) -> list[str]:
        """Генерация DDL, который создает или пересоздает VIEW."""
        replace_sql = "OR REPLACE" if replace else ""
        on_cluster = f"ON CLUSTER {cluster_name}" if cluster_name is not None else ""
        result = f"CREATE {replace_sql} VIEW {schema_name}.{name} {on_cluster} AS ({sql_expression})"
        return [result]

    def _get_delete_view_sql(self, schema_name: str, name: str, cluster_name: Optional[str] = None) -> str:
        """Генерация DDL, который удаляет VIEW."""
        on_cluster = f"ON CLUSTER {cluster_name}" if cluster_name is not None else ""
        is_sync = " SYNC" if settings.SYNC_CLICKHOUSE_DROP else ""
        return f"DROP VIEW IF EXISTS {schema_name}.{name} {on_cluster}{is_sync}"
