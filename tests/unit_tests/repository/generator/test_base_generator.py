from contextlib import nullcontext as does_not_raise
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import settings
from src.db.data_storage import DataStorage
from src.db.database import Database
from src.models.any_field import AnyFieldTypeEnum
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.models.database_object import DatabaseObject as DatabaseObjectModel, DbObjectTypeEnum
from src.models.dimension import DimensionTypeEnum
from src.models.measure import MeasureTypeEnum
from src.repository.generators.base_generator import GeneratorRepository
from src.repository.generators.clickhouse_generator import GeneratorClickhouseRepository
from src.repository.generators.postgresql_generator import GeneratorPostgreSQLRepository
from src.repository.utils import get_field_type_with_length


class TestGeneratorRepository:

    @pytest.mark.parametrize(
        ("expected", "index_field"),
        [
            ((12, 1, MeasureTypeEnum.FLOAT), 0),
            ((123, None, DimensionTypeEnum.STRING), 1),
            ((12, 1, AnyFieldTypeEnum.DECIMAL), 2),
        ],
    )
    def test_get_field_type_with_length(
        self, data_storages: list[DataStorage], expected: tuple, index_field: int
    ) -> None:
        assert expected == get_field_type_with_length(data_storages[0].fields[index_field])

    def test_get_default_value_by_field_returns_none_for_nullable_measure(
        self, data_storages: list[DataStorage]
    ) -> None:
        field = data_storages[0].fields[0]
        field.allow_null_values_local = True

        assert (
            GeneratorRepository.get_default_value_by_field(field, DatabaseTypeEnum.CLICKHOUSE)
            is None
        )

    @pytest.mark.parametrize(
        ("expected", "index_data_storage", "database_object", "not_exists", "expected_raise"),
        [
            (
                (
                    "CREATE TABLE test_schema1.test_dso1 ON CLUSTER test_cluster ( "
                    + '"test_field1_table" Float32 DEFAULT 0,"test_field2_table" '
                    + 'String DEFAULT \'\',"test_field3_table" Decimal(12,1) DEFAULT 0,',
                    [],
                ),
                0,
                DatabaseObjectModel(
                    name="test_dso1",
                    schema_name="test_schema1",
                    type=DbObjectTypeEnum.TABLE,
                    specific_attributes=[],
                ),
                False,
                does_not_raise(),
            ),
            (
                (
                    "CREATE TABLE IF NOT EXISTS test_schema1.test_dso1 ON CLUSTER "
                    + 'test_cluster ( "test_field1_table" Float32 DEFAULT 0,"test_field2_table" '
                    + 'String DEFAULT \'\',"test_field3_table" Decimal(12,1) DEFAULT 0,',
                    [],
                ),
                0,
                DatabaseObjectModel(
                    name="test_dso1",
                    schema_name="test_schema1",
                    type=DbObjectTypeEnum.TABLE,
                    specific_attributes=[],
                ),
                True,
                does_not_raise(),
            ),
            (
                (
                    "",
                    [],
                ),
                1,
                DatabaseObjectModel(
                    name="test_dso1",
                    schema_name="test_schema1",
                    type=DbObjectTypeEnum.TABLE,
                    specific_attributes=[],
                ),
                True,
                pytest.raises(ValueError),
            ),
        ],
    )
    def test_get_create_table_sql_wihout_pk(
        self,
        monkeypatch: pytest.MonkeyPatch,
        expected: tuple,
        index_data_storage: int,
        database_object: DatabaseObjectModel,
        not_exists: bool,
        expected_raise: pytest.RaisesExc,
        data_storages: list[DataStorage],
    ) -> None:
        if index_data_storage == 1:
            data_storages[index_data_storage].fields = []
        if data_storages[index_data_storage].models[0].database.type == DatabaseTypeEnum.CLICKHOUSE:
            monkeypatch.setattr(
                GeneratorRepository, "_get_table_field_type", GeneratorClickhouseRepository._get_table_field_type
            )
            monkeypatch.setattr(
                GeneratorRepository, "_get_create_table_prefix", GeneratorClickhouseRepository._get_create_table_prefix
            )
        else:
            monkeypatch.setattr(
                GeneratorRepository, "_get_table_field_type", GeneratorPostgreSQLRepository._get_table_field_type
            )
            monkeypatch.setattr(
                GeneratorRepository, "_get_create_table_prefix", GeneratorPostgreSQLRepository._get_create_table_prefix
            )
        model = data_storages[index_data_storage].models[0]
        database = DatabaseModel.model_validate(model.database)
        with expected_raise:
            assert expected == GeneratorRepository._get_create_table_request_sql_wihout_pk(
                data_storages[index_data_storage], database_object, database, not_exists
            )

    @pytest.mark.parametrize(
        ("index_data_storage", "generate_objects"),
        [(0, True), (0, False)],
    )
    async def test_create_datastorage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        data_storages: list[DataStorage],
        index_data_storage: int,
        generate_objects: bool,
    ) -> None:
        async def mock_execute_ddl(queries: str | list[str], database: DatabaseModel) -> None:
            return None

        monkeypatch.setattr(settings, "ENABLE_GENERATE_OBJECTS", generate_objects)
        if data_storages[index_data_storage].models[0].database.type == DatabaseTypeEnum.CLICKHOUSE:
            monkeypatch.setattr(
                GeneratorRepository, "_get_create_table_sql", GeneratorClickhouseRepository._get_create_table_sql
            )
        else:
            monkeypatch.setattr(
                GeneratorRepository, "_get_create_table_sql", GeneratorPostgreSQLRepository._get_create_table_sql
            )
        monkeypatch.setattr(GeneratorRepository, "_execute_DDL", mock_execute_ddl)
        await GeneratorRepository.create_datastorage(
            data_storages[index_data_storage], data_storages[index_data_storage].models[0]
        )

    @pytest.mark.parametrize(
        (
            "database_objects_params",
            "dso_field",
            "phis_table_fields",
            "is_possible_to_drop",
            "expected_result",
            "execute_query",
            "expected_exception",
            "enable_delete_column",
        ),
        [
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Int32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field", "data_type": "Float32", "is_primary_key": False},
                False,
                (True, True, ["modify_column"]),
                True,
                does_not_raise(),
                True,
            ),
            (
                (),
                {"sql_name": "test_field", "data_type": "Int32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field"},
                False,
                (True, True, ["modify_column"]),
                False,
                pytest.raises(ValueError),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": None, "data_type": "Int32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field", "data_type": "Float32", "is_primary_key": False},
                False,
                (True, True, ["modify_column"]),
                True,
                pytest.raises(ValueError),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Int32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field", "data_type": "Int32", "is_primary_key": False},
                False,
                (True, False, ["modify_column"]),
                False,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Int32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field", "data_type": "Int32", "is_primary_key": False},
                False,
                (False, False, []),
                False,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Int32", "is_primary_key": True, "default_value": 0},
                {"name": "test_field", "data_type": "Int32", "is_primary_key": False},
                False,
                (True, False, []),
                True,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field1", "data_type": "Float32", "is_primary_key": True, "default_value": 0},
                {"name": "test_field1", "data_type": "Int32", "is_primary_key": True},
                False,
                (True, False, []),
                True,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Float32", "is_primary_key": True, "default_value": 0},
                {"name": "test_field1", "data_type": "Int32", "is_primary_key": True},
                False,
                (True, False, []),
                True,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Float32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field1", "data_type": "Int32", "is_primary_key": True},
                False,
                (True, False, []),
                True,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Float32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field1", "data_type": "Int32", "is_primary_key": False},
                False,
                (True, False, []),
                True,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Float32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field1", "data_type": "Int32", "is_primary_key": False},
                True,
                (False, False, []),
                True,
                does_not_raise(),
                True,
            ),
            (
                (
                    (DbObjectTypeEnum.TABLE, "test_table", "test_schema"),
                    (DbObjectTypeEnum.DISTRIBUTED_TABLE, "test_distr", "test_schema"),
                ),
                {"sql_name": "test_field", "data_type": "Float32", "is_primary_key": False, "default_value": 0},
                {"name": "test_field1", "data_type": "Int32", "is_primary_key": False},
                False,
                (False, False, []),
                False,
                does_not_raise(),
                False,
            ),
        ],
    )
    async def test_alter_datastorage_by_comparing_with_meta(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        database_objects_params: tuple[tuple[DbObjectTypeEnum, str, str]],
        dso_field: dict[str, Any],
        phis_table_fields: dict[str, Any],
        is_possible_to_drop: bool,
        expected_result: tuple[bool, bool, list[str]],
        execute_query: bool,
        expected_exception: pytest.RaisesExc,
        enable_delete_column: bool,
    ) -> None:
        database_objects = []
        for database_object_params in database_objects_params:
            mock_database_object = MagicMock()
            mock_database_object.configure_mock(
                name=database_object_params[1], type=database_object_params[0], schema_name=database_object_params[2]
            )
            database_objects.append(mock_database_object)
        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=database_objects)
        mock_phis_table_fields = {phis_table_fields["name"]: phis_table_fields}
        mock_get_table_field_description = AsyncMock(return_value=mock_phis_table_fields)
        mock_datastorage = MagicMock()
        field = MagicMock()
        field.configure_mock(
            name=dso_field["sql_name"],
            sql_name=dso_field["sql_name"],
            is_key=dso_field["is_primary_key"],
        )
        mock_model = MagicMock()
        mock_model.configure_mock(database=databases[0])
        mock_datastorage.configure_mock(name="test_datastorage", fields=[field])
        mock_get_table_field_type = MagicMock(return_value=(dso_field["data_type"]))
        mock_get_modify_column_sql = MagicMock(return_value="mock_modify_column_sql")
        mock_get_default_value_by_field = MagicMock(return_value=dso_field["default_value"])
        mock_get_create_column_sql = MagicMock(return_value="mock_create_column_sql")
        mock_is_possible_to_drop_column = AsyncMock(return_value=is_possible_to_drop)
        mock_get_drop_column_sql = MagicMock(return_value="mock_drop_column_sql")
        mock_get_alter_database_objects_sql_expressions = AsyncMock(return_value=expected_result[2])
        mock_execute_ddl = AsyncMock(return_value=None)
        # Устанавливаем моки для функций, которые вызываются внутри тестируемой функции
        monkeypatch.setattr(
            "src.repository.generators.base_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_table_field_description",
            mock_get_table_field_description,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_table_field_type",
            mock_get_table_field_type,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_modify_column_sql",
            mock_get_modify_column_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.get_default_value_by_field",
            mock_get_default_value_by_field,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_create_column_sql",
            mock_get_create_column_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._is_possible_to_drop_column",
            mock_is_possible_to_drop_column,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_drop_column_sql",
            mock_get_drop_column_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.get_alter_database_objects_sql_expressions",
            mock_get_alter_database_objects_sql_expressions,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._execute_DDL", mock_execute_ddl
        )

        # Вызываем тестируемую функцию
        with expected_exception:
            result = await GeneratorRepository.alter_datastorage_by_comparing_with_meta(
                mock_datastorage,
                mock_model,
                enable_delete_column=enable_delete_column,
                execute_query=execute_query,
            )

            # Проверяем результат
            assert result == expected_result

    async def test_is_possible_to_drop_data_storage(
        self, monkeypatch: pytest.MonkeyPatch, databases: list[Database]
    ) -> None:
        # Создаем моки для необходимых объектов
        mock_data_storage = MagicMock()
        mock_model = MagicMock()
        mock_database = databases[0]
        mock_database_objects_model = MagicMock()
        mock_possible_to_drop = MagicMock()
        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=mock_database_objects_model)
        mock_possible_to_drop = AsyncMock(return_value=True)
        # Устанавливаем возвращаемые значения для моков
        mock_model.configure_mock(database=mock_database)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._is_possible_to_drop",
            mock_possible_to_drop,
        )
        # Вызываем тестируемую функцию
        result = await GeneratorRepository.is_possible_to_drop_data_storage(mock_data_storage, mock_model)

        # Проверяем, что функция возвращает ожидаемый результат
        assert result

    @pytest.mark.parametrize(
        ("schema_name", "expect_exception"),
        [
            ("schema_name", does_not_raise()),
            (None, pytest.raises(ValueError)),
        ],
    )
    async def test_delete_composite(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        schema_name: Optional[str],
        expect_exception: pytest.RaisesExc,
    ) -> None:
        composite = MagicMock()
        model = MagicMock()
        model.configure_mock(database=databases[0])
        database_object = MagicMock()
        database_object.configure_mock(schema_name=schema_name, name="name")
        database_objects = [database_object]
        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=database_objects)

        mock_get_delete_view_sql = MagicMock(return_value="sql_expression")
        monkeypatch.setattr("src.repository.generators.base_generator.settings.ENABLE_GENERATE_OBJECTS", True)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_delete_view_sql",
            mock_get_delete_view_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._execute_DDL",
            AsyncMock(),
        )
        with expect_exception:
            assert await GeneratorRepository.delete_composite(composite, model) == "sql_expression"

    @pytest.mark.parametrize(
        ("schema_name", "expect_exception"),
        [
            ("schema_name", does_not_raise()),
            (None, pytest.raises(ValueError)),
        ],
    )
    async def test_update_composite(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        schema_name: Optional[str],
        expect_exception: pytest.RaisesExc,
    ) -> None:
        composite = MagicMock()
        model = MagicMock()
        model.configure_mock(database=databases[0])
        database_object = MagicMock()
        database_object.configure_mock(schema_name=schema_name, name="name")
        database_objects = [database_object]
        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=database_objects)

        mock_get_create_view_sql = MagicMock(return_value="sql_expression")
        monkeypatch.setattr("src.repository.generators.base_generator.settings.ENABLE_GENERATE_OBJECTS", True)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_create_view_sql",
            mock_get_create_view_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._execute_DDL",
            AsyncMock(),
        )
        with expect_exception:
            assert await GeneratorRepository.update_composite(composite, model, "expression") == "sql_expression"

    @pytest.mark.parametrize(
        ("schema_name", "expect_exception"),
        [
            ("schema_name", does_not_raise()),
            (None, pytest.raises(ValueError)),
        ],
    )
    async def test_create_composite(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        schema_name: Optional[str],
        expect_exception: pytest.RaisesExc,
    ) -> None:
        composite = MagicMock()
        model = MagicMock()
        model.configure_mock(database=databases[0], name="model_name")
        database_object = MagicMock()
        database_object.configure_mock(schema_name=schema_name, name="name", models=[model])
        database_objects = [database_object]
        composite.configure_mock(database_objects=database_objects, name="test_composite")
        mock_get_create_view_sql = MagicMock(return_value="sql_expression")
        monkeypatch.setattr("src.repository.generators.base_generator.settings.ENABLE_GENERATE_OBJECTS", True)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_create_view_sql",
            mock_get_create_view_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._execute_DDL",
            AsyncMock(),
        )
        with expect_exception:
            assert await GeneratorRepository.create_composite(composite, model, "expression", True) == "sql_expression"

    @pytest.mark.parametrize(
        (
            "sql_expression",
            "execute_query",
            "context",
            "exc",
        ),
        [
            (["test_create_sql"], True, does_not_raise(), None),
            ([], True, does_not_raise(), None),
            ([], False, does_not_raise(), None),
            (["test_create_sql"], True, pytest.raises(Exception), Exception),
        ],
    )
    async def test_create_datastorage_with_mocks(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        sql_expression: list[str],
        execute_query: bool,
        context: pytest.RaisesExc,
        exc: Any,
    ) -> None:
        datastorage = MagicMock()
        datastorage.configure_mock(name="test", tenant_id="tenant_id")
        model = MagicMock()
        model.configure_mock(database=databases[0], name="model_name")
        mock_get_create_table_sql = MagicMock(return_value=sql_expression)
        mock_execute_DDL = AsyncMock(side_effect=exc)
        monkeypatch.setattr("src.repository.generators.base_generator.settings.ENABLE_GENERATE_OBJECTS", True)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_create_table_sql",
            mock_get_create_table_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._execute_DDL",
            mock_execute_DDL,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.delete_datastorage",
            AsyncMock(),
        )
        with context:
            assert (
                await GeneratorRepository.create_datastorage(datastorage, model, True, True, execute_query)
                == sql_expression
            )

    @pytest.mark.parametrize(
        ("сheck_possible_to_drop_data_storage", "is_possible_to_drop", "fields", "context"),
        [
            (True, [1], True, does_not_raise()),
            (False, [1], True, does_not_raise()),
            (True, [], True, pytest.raises(ValueError)),
            (True, [1], False, pytest.raises(ValueError)),
        ],
    )
    async def test_recreate_data_storage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        сheck_possible_to_drop_data_storage: bool,
        is_possible_to_drop: bool,
        fields: list,
        context: pytest.RaisesExc,
    ) -> None:
        model = MagicMock()
        model.configure_mock(name="test")
        datastorage = MagicMock()
        datastorage.configure_mock(name="test", fields=fields)
        database_object = MagicMock()
        database_object.configure_mock(schema_name="test", name="name")
        database_objects = [database_object]
        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=database_objects)
        mock_delete_datastorage = AsyncMock(return_value=["test_delete_sql"])
        mock_create_datastorage = AsyncMock(return_value=["test_create_sql"])
        monkeypatch.setattr(
            "src.repository.generators.base_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        mock_is_possible_to_drop_data_storage = AsyncMock(return_value=is_possible_to_drop)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.is_possible_to_drop_data_storage",
            mock_is_possible_to_drop_data_storage,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.delete_datastorage",
            mock_delete_datastorage,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.create_datastorage",
            mock_create_datastorage,
        )
        with context:
            assert await GeneratorRepository.recreate_data_storage(
                datastorage, model, сheck_possible_to_drop_data_storage
            ) == ["test_delete_sql", "test_create_sql"]

    @pytest.mark.parametrize(
        (
            "is_exist_datastorage",
            "is_changed",
            "is_sql_expressions_completed",
            "is_possible_to_drop",
            "alter_sql_expressions",
            "result_expression",
            "context",
        ),
        [
            (False, True, True, True, [], ["recreate"], does_not_raise()),
            (True, False, False, False, [], [], does_not_raise()),
            (True, True, True, False, ["altering"], ["altering"], does_not_raise()),
            (True, True, False, True, [], ["altering"], does_not_raise()),
            (True, True, False, False, [], ["altering"], pytest.raises(ValueError)),
        ],
    )
    async def test_update_datastorage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        is_exist_datastorage: bool,
        is_changed: bool,
        is_sql_expressions_completed: bool,
        is_possible_to_drop: bool,
        alter_sql_expressions: list[str],
        result_expression: list[str],
        context: pytest.RaisesExc,
    ) -> None:
        data_storage = MagicMock()
        model = MagicMock()
        data_storage.configure_mock(name="test")
        monkeypatch.setattr("src.repository.generators.base_generator.settings.ENABLE_GENERATE_OBJECTS", True)
        mock_is_exist_datastorage = AsyncMock(return_value=is_exist_datastorage)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.is_exist_datastorage",
            mock_is_exist_datastorage,
        )
        mock_recreate_data_storage = AsyncMock(return_value=result_expression)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.recreate_data_storage",
            mock_recreate_data_storage,
        )
        mock_alter_datastorage_by_comparing_with_meta = AsyncMock(
            return_value=(is_changed, is_sql_expressions_completed, alter_sql_expressions)
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.alter_datastorage_by_comparing_with_meta",
            mock_alter_datastorage_by_comparing_with_meta,
        )
        mock_is_possible_to_drop_data_storage = AsyncMock(return_value=is_possible_to_drop)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.is_possible_to_drop_data_storage",
            mock_is_possible_to_drop_data_storage,
        )
        with context:
            assert await GeneratorRepository.update_datastorage(data_storage, model) == result_expression

    @pytest.mark.parametrize(
        ("check_possible_delete", "context"),
        [
            (False, does_not_raise()),
            (True, pytest.raises(ValueError)),
        ],
    )
    async def test_get_delete_db_objects_sql_and_check_possible_delete(
        self, monkeypatch: pytest.MonkeyPatch, check_possible_delete: bool, context: pytest.RaisesExc
    ) -> None:
        database_objects = MagicMock()
        database = MagicMock()
        mock_get_delete_db_objects_sql = MagicMock(return_value=["test"])
        mock_is_possible_to_drop = AsyncMock(return_value=False)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_delete_db_objects_sql",
            mock_get_delete_db_objects_sql,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._is_possible_to_drop",
            mock_is_possible_to_drop,
        )
        with context:
            assert await GeneratorRepository._get_delete_db_objects_sql_and_check_possible_delete(
                database_objects,
                database,
                check_possible_delete=check_possible_delete,
            ) == ["test"]

    @pytest.mark.parametrize(
        (
            "data_storage",
            "database_objects_model",
            "database_objects_model_from_filters",
            "delete_expressions",
            "execute_query",
            "context",
            "exc",
            "result",
        ),
        [
            (MagicMock(), None, None, [], True, pytest.raises(ValueError), None, []),
            (None, MagicMock(), None, ["test"], True, does_not_raise(), None, ["test"]),
            (None, MagicMock(), None, [], True, does_not_raise(), None, []),
            (None, MagicMock(), None, ["test"], False, does_not_raise(), None, ["test"]),
            (MagicMock(), MagicMock(), None, ["test"], True, pytest.raises(Exception), Exception, []),
        ],
    )
    async def test_delete_datastorage(
        self,
        monkeypatch: pytest.MonkeyPatch,
        databases: list[Database],
        data_storage: Any,
        database_objects_model: Optional[list[DatabaseObjectModel]],
        database_objects_model_from_filters: Any,
        delete_expressions: list[str],
        execute_query: bool,
        context: pytest.RaisesExc,
        exc: Any,
        result: list[str],
    ) -> None:
        monkeypatch.setattr("src.repository.generators.base_generator.settings.ENABLE_GENERATE_OBJECTS", True)
        mock_model = MagicMock()
        mock_model.configure_mock(database=databases[0], name="test")
        mock_get_filtred_database_object_by_data_storage = MagicMock(return_value=database_objects_model_from_filters)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.get_filtred_database_object_by_data_storage",
            mock_get_filtred_database_object_by_data_storage,
        )
        mock_get_delete_db_objects_sql_and_check_possible_delete = AsyncMock(return_value=delete_expressions)
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._get_delete_db_objects_sql_and_check_possible_delete",
            mock_get_delete_db_objects_sql_and_check_possible_delete,
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository._execute_DDL",
            AsyncMock(side_effect=exc),
        )
        monkeypatch.setattr(
            "src.repository.generators.base_generator.GeneratorRepository.create_datastorage",
            AsyncMock(return_value=None),
        )
        with context:
            assert (
                await GeneratorRepository.delete_datastorage(
                    data_storage, mock_model, database_objects_model=database_objects_model, execute_query=execute_query
                )
                == result
            )
