from contextlib import nullcontext as does_not_raise
from typing import Optional, Type
from unittest.mock import AsyncMock, MagicMock

import pytest
from clickhouse_connect.driver.exceptions import OperationalError

from src.db.data_storage import DataStorage
from src.db.database import Database
from src.models.database import Database as DatabaseModel
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DbObjectTypeEnum
from src.repository.generators import clickhouse_generator
from src.repository.generators.clickhouse_generator import GeneratorClickhouseRepository
from tests.unit_tests.fixtures.data_storage import data_storage_model_list
from tests.unit_tests.fixtures.database import database_model_list


class ResultMock:

    def __init__(self) -> None:
        self.result_rows = [1, 2, 3]


class MockClickHouseClient:

    def __init__(self, exception: Optional[Type[Exception]]) -> None:
        self.exception = exception

    async def query(self, query: str) -> ResultMock:
        if self.exception:
            raise self.exception
        return ResultMock()


class TestGeneratorClickhouseRepository:

    @pytest.mark.parametrize(
        ("expected", "index_data_storage", "index_field", "without_null"),
        [
            ("Float32", 0, 0, True),
            ("Float32 DEFAULT 0", 0, 0, False),
            ("String", 0, 1, True),
            ("Decimal(12,1)", 0, 2, True),
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
        res = GeneratorClickhouseRepository._get_table_field_type(
            data_storages[index_data_storage].fields[index_field],
            database_model_list[0].type,
            without_null,
        )
        assert res == expected

    @pytest.mark.parametrize(
        ("queries", "exception"),
        [
            (["query1", "query2"], None),
            ("query1", None),
            ("query1", OperationalError),
            ("query1", ConnectionRefusedError),
        ],
    )
    async def test_execute_DDL(
        self, monkeypatch: pytest.MonkeyPatch, queries: str | list[str], exception: Optional[Type[Exception]]
    ) -> None:
        async def mock_get_client(database: DatabaseModel) -> MockClickHouseClient:
            return MockClickHouseClient(exception)

        monkeypatch.setattr(clickhouse_generator, "get_client", mock_get_client)
        if exception is None:
            await GeneratorClickhouseRepository._execute_DDL(queries, database_model_list[0])
        else:
            with pytest.raises(exception):
                await GeneratorClickhouseRepository._execute_DDL(queries, database_model_list[0])

    @pytest.mark.parametrize(
        ("query", "exception"),
        [
            ("query1", None),
            ("query1", OperationalError),
            ("query1", ConnectionRefusedError),
        ],
    )
    async def test_get_data_query(
        self, monkeypatch: pytest.MonkeyPatch, query: str, exception: Optional[Type[Exception]]
    ) -> None:
        async def mock_get_client(database: DatabaseModel) -> MockClickHouseClient:
            return MockClickHouseClient(exception)

        monkeypatch.setattr(clickhouse_generator, "get_client", mock_get_client)
        if exception is None:
            assert await GeneratorClickhouseRepository._get_data_query(query, database_model_list[0]) == [1, 2, 3]
        else:
            with pytest.raises(exception):
                await GeneratorClickhouseRepository._get_data_query(query, database_model_list[0])

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
                ["CREATE OR REPLACE VIEW test.test1  AS (SELECT * FROM test.test)"],
            ),
            (
                "test",
                "test2",
                "SELECT * FROM test.test",
                "cluster",
                False,
                ["CREATE  VIEW test.test2 ON CLUSTER cluster AS (SELECT * FROM test.test)"],
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
            GeneratorClickhouseRepository._get_create_view_sql(
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
                "DROP VIEW IF EXISTS test.test1  SYNC",
            ),
            (
                "test",
                "test2",
                "cluster",
                "DROP VIEW IF EXISTS test.test2 ON CLUSTER cluster SYNC",
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
            GeneratorClickhouseRepository._get_delete_view_sql(
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
                    DatabaseObjectModel(
                        name="test_distr",
                        schema_name="test_schema",
                        type=DbObjectTypeEnum.DISTRIBUTED_TABLE,
                        specific_attributes=[],
                    ),
                ],
                "cluster_name",
                True,
                [
                    "DROP TABLE IF EXISTS test_schema.test_distr ON CLUSTER cluster_name SYNC",
                    "DROP TABLE IF EXISTS test_schema.test ON CLUSTER cluster_name SYNC",
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
                    "DROP TABLE IF EXISTS test_schema.test  SYNC",
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
            GeneratorClickhouseRepository._get_delete_db_objects_sql(
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
                "CREATE TABLE IF NOT EXISTS test_schema1.test_dso1 ON CLUSTER test_cluster ( ",
                "test_cluster",
            ),
        ],
    )
    def test_get_create_table_prefix(
        self,
        database_object: DatabaseObjectModel,
        not_exists: bool,
        expected: list[str],
        default_cluster_name: str,
    ) -> None:
        assert (
            GeneratorClickhouseRepository._get_create_table_prefix(database_object, default_cluster_name, not_exists)
            == expected
        )

    @pytest.mark.parametrize(
        ("index_data_storage", "not_exists", "expected"),
        [
            (
                0,
                False,
                [
                    """CREATE TABLE test_schema1.test_dso1 ON CLUSTER test_cluster ( "test_field1_table" Float32"""
                    + """ DEFAULT 0,"test_field2_table" String DEFAULT '',"test_field3_table" Decimal(12,1) DEFAULT 0) ENGINE = Log()""",
                    "CREATE TABLE test_schema1.test_dso1_distr ON CLUSTER test_cluster "
                    + """("test_field1_table" Float32 DEFAULT 0,"test_field2_table" String """
                    + """DEFAULT \'\',"test_field3_table" Decimal(12,1) DEFAULT 0) ENGINE = """
                    + "Distributed(test_cluster, test_schema1, test_dso1, rand())",
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
        assert expected == GeneratorClickhouseRepository._get_create_table_sql(
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

        monkeypatch.setattr(GeneratorClickhouseRepository, "_get_data_query", get_data_query_mock)
        database_objects = data_storage_model_list[0].database_objects
        model = data_storages[0].models[0]
        if database_objects is None or model is None or model.database is None:
            raise ValueError("Not valid data for test")
        database = DatabaseModel.model_validate(model.database)
        assert expected == await GeneratorClickhouseRepository._is_possible_to_drop(database_objects, database)

    @pytest.mark.parametrize(
        ("database_objects_params", "expected_sql", "prev_sharding_key", "new_sharding_key", "expected_exception"),
        [
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                [
                    (
                        "ALTER TABLE test_schema.test_table ON CLUSTER test_cluster "
                        "ADD COLUMN test_column int,DROP COLUMN test_column1"
                    ),
                    (
                        "ALTER TABLE test_schema.test_distr ON CLUSTER test_cluster "
                        "ADD COLUMN test_column int,DROP COLUMN test_column1"
                    ),
                ],
                "test_sharding_key",
                "test_sharding_key",
                does_not_raise(),
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DICTIONARY, "test_dict", "test_schema"),
                ),
                [
                    (
                        "ALTER TABLE test_schema.test_table ON CLUSTER test_cluster "
                        "ADD COLUMN test_column int,DROP COLUMN test_column1"
                    ),
                    "mock create dict test_schema.test_dict",
                ],
                "test_sharding_key",
                "test_sharding_key",
                does_not_raise(),
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                [
                    "mock drop table test_schema.test_distr",
                    (
                        "ALTER TABLE test_schema.test_table ON CLUSTER test_cluster ADD COLUMN "
                        "test_column int,DROP COLUMN test_column1"
                    ),
                    "mock create table test_schema.test_distr",
                ],
                "test_sharding_key",
                "new_sharding_key",
                does_not_raise(),
            ),
            (
                (),
                [],
                "",
                "",
                pytest.raises(ValueError),
            ),
        ],
    )
    async def test_get_alter_database_objects_sql_expressions(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        database_objects_params: tuple[tuple[DbObjectTypeEnum, str, str]],
        expected_sql: list[str],
        prev_sharding_key: str,
        new_sharding_key: str,
        expected_exception: pytest.RaisesExc,
    ) -> None:
        # Создаем тестовые данные
        datastorage = MagicMock()
        datastorage.configure_mock(name="test_datastorage", sharding_key=new_sharding_key)
        model = MagicMock()
        model.configure_mock(name="test_model", database=databases[0])
        model.database.default_cluster_name = "test_cluster"
        sql_expressions = ["ADD COLUMN test_column int", "DROP COLUMN test_column1"]
        # Создаем моки для функций, которые вызываются внутри тестируемой функции
        database_objects = []
        for database_object_params in database_objects_params:
            mock_database_object = MagicMock()
            mock_database_object.configure_mock(
                name=database_object_params[1], type=database_object_params[0], schema_name=database_object_params[2]
            )
            database_objects.append(mock_database_object)

        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=database_objects)
        mock_get_sharding_key_from_table = AsyncMock(return_value=prev_sharding_key)
        mock_get_drop_table_sql = MagicMock(return_value="mock drop table test_schema.test_distr")
        mock_create_dictionary_sql = MagicMock(return_value="mock create dict test_schema.test_dict")
        mock_get_create_distributed_table_sql = MagicMock(return_value="mock create table test_schema.test_distr")

        # Заменяем оригинальные функции на моки
        monkeypatch.setattr(
            "src.repository.generators.clickhouse_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        monkeypatch.setattr(
            "src.repository.generators.clickhouse_generator.GeneratorClickhouseRepository.get_sharding_key_from_table",
            mock_get_sharding_key_from_table,
        )
        monkeypatch.setattr(
            "src.repository.generators.clickhouse_generator.GeneratorClickhouseRepository._get_drop_table_sql",
            mock_get_drop_table_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.clickhouse_generator.GeneratorClickhouseRepository._create_dictionary_sql",
            mock_create_dictionary_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.clickhouse_generator.GeneratorClickhouseRepository._get_create_distributed_table_sql",
            mock_get_create_distributed_table_sql,
        )
        with expected_exception:
            # Вызываем тестируемую функцию
            result = await GeneratorClickhouseRepository.get_alter_database_objects_sql_expressions(
                datastorage, model, sql_expressions
            )

            # Проверяем, что функция возвращает ожидаемые результаты
            assert result == expected_sql
