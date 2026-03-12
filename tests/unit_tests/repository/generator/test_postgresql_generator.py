from typing import Any, Optional, Type
from unittest.mock import MagicMock

import pytest

from src.db.data_storage import DataStorage
from src.models.database import Database as DatabaseModel
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DbObjectTypeEnum
from src.repository.generators import postgresql_generator
from src.repository.generators.postgresql_generator import GeneratorPostgreSQLRepository
from tests.unit_tests.fixtures.data_storage import data_storage_model_list
from tests.unit_tests.fixtures.database import database_model_list


class ResultMock:

    def __init__(self, rows: list) -> None:
        self.rows = rows
        self.rowcount = len(rows)

    def fetchall(self) -> list:
        return self.rows


class MockSession:

    def __init__(self, exception: Optional[Type[Exception]], rows: list) -> None:
        self.exception = exception
        self.rows = rows

    async def commit(self) -> None:
        return None

    async def execute(self, statement: Any, params: Optional[dict] = None) -> ResultMock:
        if self.exception:
            raise self.exception
        return ResultMock(self.rows)


class MockAsyncSessionMaker:

    def __init__(self, exception: Optional[Type[Exception]], rows: list) -> None:
        self.exception = exception
        self.rows = rows

    def __call__(self) -> "MockAsyncSessionMaker":
        return self

    async def __aenter__(self) -> MockSession:
        return MockSession(self.exception, self.rows)

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class TestGeneratorPostgreSQLRepository:

    @pytest.mark.parametrize(
        ("expected", "index_data_storage", "index_field", "without_null"),
        [
            ("Float32", 0, 0, True),
            ("Float32 NOT NULL DEFAULT 0", 0, 0, False),
            ("String", 0, 1, True),
            ("Decimal(13,1)", 0, 2, True),
        ],
    )
    def test_get_table_field_type(
        self,
        data_storages: list[DataStorage],
        expected: str,
        index_data_storage: int,
        index_field: int,
        without_null: bool,
    ) -> None:
        res = GeneratorPostgreSQLRepository._get_table_field_type(
            data_storages[index_data_storage].fields[index_field],
            database_model_list[0].type,
            without_null,
        )
        assert res == expected

    def test_get_table_field_type_for_nullable_measure_field(self, data_storages: list[DataStorage]) -> None:
        field = data_storages[0].fields[0]
        field.allow_null_values_local = True

        assert GeneratorPostgreSQLRepository._get_table_field_type(field, database_model_list[0].type, True) == "Float32"
        assert GeneratorPostgreSQLRepository._get_table_field_type(field, database_model_list[0].type, False) == "Float32"

    def test_get_modify_column_sql_for_nullability_toggle(self) -> None:
        assert GeneratorPostgreSQLRepository._get_modify_column_sql(
            "value",
            "float4",
            is_nullable=True,
            current_field_type="float4",
            current_is_nullable=False,
        ) == ['ALTER COLUMN "value" DROP DEFAULT;', 'ALTER COLUMN "value" DROP NOT NULL']
        assert GeneratorPostgreSQLRepository._get_modify_column_sql(
            "value",
            "float4",
            default_value="0",
            is_nullable=False,
            current_field_type="float4",
            current_is_nullable=True,
        ) == [
            'ALTER COLUMN "value" DROP DEFAULT;',
            "__UPDATE_NULLS__|value|0",
            'ALTER COLUMN "value" SET NOT NULL',
            'ALTER COLUMN "value" SET DEFAULT 0',
        ]

    @pytest.mark.parametrize(
        ("queries", "rows", "exception"),
        [
            (["query1", "query2"], [1, 2, 3], None),
            ("query1", [], None),
            ("query1", [], Exception),
            ("query1", [], ConnectionRefusedError),
        ],
    )
    async def test_execute_DDL(
        self,
        monkeypatch: pytest.MonkeyPatch,
        queries: str | list[str],
        rows: list,
        exception: Optional[Type[Exception]],
    ) -> None:
        class MockPostgreSQLClient:

            def __init__(self, database: DatabaseModel) -> None:
                self.exception = exception
                self.rows = rows

            async def get_not_pg_is_in_recovery(self) -> tuple[Any, Any]:
                return MagicMock(), MockAsyncSessionMaker(self.exception, self.rows)

        monkeypatch.setattr(postgresql_generator, "DatabaseConnector", MockPostgreSQLClient)
        if exception is None:
            await GeneratorPostgreSQLRepository._execute_DDL(queries, database_model_list[0])
        else:
            with pytest.raises(exception):
                await GeneratorPostgreSQLRepository._execute_DDL(queries, database_model_list[0])

    @pytest.mark.parametrize(
        ("query", "rows", "exception"),
        [
            ("query1", [(1, 2, 3), (1, 2, 3), (1, 2, 3)], None),
            ("query1", [], None),
            ("query1", [], Exception),
            ("query1", [], ConnectionRefusedError),
        ],
    )
    async def test_get_data_query(
        self, monkeypatch: pytest.MonkeyPatch, query: str, rows: list, exception: Optional[Type[Exception]]
    ) -> None:
        class MockPostgreSQLClient:

            def __init__(self, database: DatabaseModel) -> None:
                self.exception = exception
                self.rows = rows

            async def get_not_pg_is_in_recovery(self) -> tuple[Any, Any]:
                return MagicMock(), MockAsyncSessionMaker(self.exception, self.rows)

        monkeypatch.setattr(postgresql_generator, "DatabaseConnector", MockPostgreSQLClient)
        if exception is None:
            assert await GeneratorPostgreSQLRepository._get_data_query(query, database_model_list[0]) == rows
        else:
            with pytest.raises(exception):
                await GeneratorPostgreSQLRepository._get_data_query(query, database_model_list[0])

    async def test_find_views_by_table_matches_only_exact_table_names(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def get_data_query_mock(query: str, database: DatabaseModel, params: Optional[dict] = None) -> list:
            assert "LIKE" not in query
            assert params == {"schema_name": "test_schema1"}
            return [
                (
                    "test_schema1",
                    "exact_view",
                    "CREATE VIEW test_schema1.exact_view AS SELECT * FROM test_schema1.test_dso1",
                ),
                (
                    "test_schema1",
                    "partial_view",
                    "CREATE VIEW test_schema1.partial_view AS SELECT * FROM test_schema1.test_dso11",
                ),
            ]

        monkeypatch.setattr(GeneratorPostgreSQLRepository, "_get_data_query", get_data_query_mock)

        result = await GeneratorPostgreSQLRepository.find_views_by_table(
            database_model_list[1], "test_schema1", ["test_dso1"]
        )

        assert result == [
            {
                "view_schema": "test_schema1",
                "view_name": "exact_view",
                "view_definition": "CREATE VIEW test_schema1.exact_view AS SELECT * FROM test_schema1.test_dso1",
            }
        ]

    @pytest.mark.parametrize(
        (
            "schema_name",
            "name",
            "sql_expression",
            "cluster_name",
            "replace",
            "excepted",
        ),
        [
            (
                "test",
                "test1",
                "SELECT * FROM test.test",
                None,
                True,
                ["DROP VIEW IF EXISTS test.test1", "CREATE VIEW test.test1 AS (SELECT * FROM test.test)"],
            ),
            (
                "test",
                "test2",
                "SELECT * FROM test.test",
                "cluster",
                False,
                ["CREATE VIEW test.test2 AS (SELECT * FROM test.test)"],
            ),
        ],
    )
    def test_get_create_view_sql(
        self,
        schema_name: str,
        name: str,
        sql_expression: str,
        cluster_name: Optional[str],
        replace: bool,
        excepted: list[str],
    ) -> None:
        assert (
            GeneratorPostgreSQLRepository._get_create_view_sql(
                schema_name,
                name,
                sql_expression,
                cluster_name,
                replace,
            )
            == excepted
        )

    @pytest.mark.parametrize(
        (
            "schema_name",
            "name",
            "cluster_name",
            "excepted",
        ),
        [
            (
                "test",
                "test1",
                None,
                "DROP VIEW IF EXISTS test.test1",
            ),
            (
                "test",
                "test2",
                "cluster",
                "DROP VIEW IF EXISTS test.test2",
            ),
        ],
    )
    def test_get_delete_view_sql(
        self,
        schema_name: str,
        name: str,
        cluster_name: Optional[str],
        excepted: str,
    ) -> None:
        assert (
            GeneratorPostgreSQLRepository._get_delete_view_sql(
                schema_name,
                name,
                cluster_name,
            )
            == excepted
        )

    @pytest.mark.parametrize(
        (
            "database_objects",
            "cluster_name",
            "exists",
            "expected",
        ),
        [
            (
                [
                    DatabaseObjectModel(
                        name="test",
                        schema_name="test_schema",
                        type=DbObjectTypeEnum.TABLE,
                        specific_attributes=[],
                    ),
                ],
                "cluster_name",
                True,
                [
                    "DROP TABLE IF EXISTS test_schema.test",
                ],
            ),
            (
                [
                    DatabaseObjectModel(
                        name="test",
                        schema_name="test_schema",
                        type=DbObjectTypeEnum.TABLE,
                        specific_attributes=[],
                    ),
                ],
                None,
                True,
                [
                    "DROP TABLE IF EXISTS test_schema.test",
                ],
            ),
        ],
    )
    def test_get_delete_table_sql(
        self,
        database_objects: list[DatabaseObjectModel],
        cluster_name: Optional[str],
        exists: bool,
        expected: list[str],
    ) -> None:
        assert (
            GeneratorPostgreSQLRepository._get_delete_db_objects_sql(
                database_objects,
                cluster_name,
                exists,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ("database_object", "not_exists", "expected", "default_cluster_name"),
        [
            (
                DatabaseObjectModel(
                    name="test_dso1",
                    schema_name="test_schema1",
                    type=DbObjectTypeEnum.TABLE,
                    specific_attributes=[],
                ),
                True,
                "CREATE TABLE IF NOT EXISTS test_schema1.test_dso1 ( ",
                None,
            ),
        ],
    )
    def test_get_create_table_prefix(
        self,
        database_object: DatabaseObjectModel,
        default_cluster_name: Optional[str],
        not_exists: bool,
        expected: list[str],
    ) -> None:
        assert (
            GeneratorPostgreSQLRepository._get_create_table_prefix(database_object, default_cluster_name, not_exists)
            == expected
        )

    @pytest.mark.parametrize(
        ("index_data_storage", "not_exists", "expected"),
        [
            (
                0,
                False,
                [
                    """CREATE TABLE test_schema1.test_dso1 ( "test_field1_table" Float32 NOT NULL DEFAULT 0,"""
                    + """"test_field2_table" String NOT NULL DEFAULT \'\',"test_field3_table" Decimal(13,1) NOT NULL DEFAULT 0)"""
                ],
            ),
        ],
    )
    def test_get_create_table_sql(
        self,
        index_data_storage: int,
        not_exists: bool,
        expected: list[str],
        data_storages: list[DataStorage],
    ) -> None:
        models = data_storages[index_data_storage].models
        database = DatabaseModel.model_validate(models[0].database)
        assert expected == GeneratorPostgreSQLRepository._get_create_table_sql(
            data_storages[index_data_storage], database, models[0].name, not_exists
        )

    @pytest.mark.parametrize(
        ("expected", "return_value"),
        [
            (False, [1, 2]),
            (True, []),
        ],
    )
    async def test_is_possible_to_drop(
        self, data_storages: list[DataStorage], monkeypatch: pytest.MonkeyPatch, expected: bool, return_value: list
    ) -> None:
        async def get_data_query_mock(query: str, database: DatabaseModel) -> list:
            return return_value

        monkeypatch.setattr(GeneratorPostgreSQLRepository, "_get_data_query", get_data_query_mock)
        database_objects = data_storage_model_list[0].database_objects
        model = data_storages[0].models[0]
        if database_objects is None or model is None or model.database is None:
            raise ValueError("Not valid data for test")
        database = DatabaseModel.model_validate(model.database)
        assert expected == await GeneratorPostgreSQLRepository._is_possible_to_drop(database_objects, database)

    async def test_get_alter_database_objects_sql_expressions(
        self, monkeypatch: pytest.MonkeyPatch, data_storages: list[DataStorage]
    ) -> None:
        # Создаем тестовые данные
        datastorage = data_storages[0]
        model = datastorage.models[0]
        sql_expressions: list[str] = ["ADD COLUMN test_column int4", "DROP COLUMN test_column1"]

        mock_get_filtred_database_object_by_data_storage = MagicMock(
            return_value=data_storage_model_list[0].database_objects
        )
        monkeypatch.setattr(
            "src.repository.generators.postgresql_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )

        # Вызываем функцию и проверяем результат
        result: list[str] = await GeneratorPostgreSQLRepository.get_alter_database_objects_sql_expressions(
            datastorage, model, sql_expressions
        )
        assert result == [
            "ALTER TABLE test_schema1.test_dso1 ADD COLUMN test_column int4",
            "ALTER TABLE test_schema1.test_dso1 DROP COLUMN test_column1",
            "ALTER TABLE test_schema1.test_dso1_distr ADD COLUMN test_column int4",
            "ALTER TABLE test_schema1.test_dso1_distr DROP COLUMN test_column1",
        ]

    async def test_get_alter_database_objects_sql_expressions_renders_update_nulls(
        self, monkeypatch: pytest.MonkeyPatch, data_storages: list[DataStorage]
    ) -> None:
        datastorage = data_storages[0]
        model = datastorage.models[0]
        sql_expressions = ["__UPDATE_NULLS__|value|0"]

        monkeypatch.setattr(
            "src.repository.generators.postgresql_generator.get_filtred_database_object_by_data_storage",
            MagicMock(return_value=data_storage_model_list[0].database_objects),
        )

        result = await GeneratorPostgreSQLRepository.get_alter_database_objects_sql_expressions(
            datastorage, model, sql_expressions
        )

        assert result == [
            'UPDATE test_schema1.test_dso1 SET "value" = 0 WHERE "value" IS NULL',
            'UPDATE test_schema1.test_dso1_distr SET "value" = 0 WHERE "value" IS NULL',
        ]
