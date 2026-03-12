import copy
from contextlib import nullcontext as does_not_raise, suppress
from typing import Any, Optional, Type

import pytest
from pandas import Series
from sqlalchemy import func, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.data_storage import DataStorage
from src.db.database_object import DatabaseObject, DatabaseObjectRelation
from src.db.model import Model
from src.models.data_storage import (
    DataStorage as DataStorageModel,
    DataStorageCreateRequest as DataStorageCreateRequestModel,
    DataStorageEditRequest as DataStorageEditRequestModel,
    DataStorageEnum,
    DataStorageFieldRequest as DataStorageFieldRequestModel,
)
from src.models.database_object import DatabaseObjectRelationTypeEnum, DbObjectTypeEnum
from src.models.field import BaseFieldType, BaseFieldTypeEnum, SemanticType
from src.models.label import Label, LabelType, Language
from src.models.request_params import DataStorageFieldsFileColumnEnum, DataStorageFileColumnEnum
from src.models.tenant import SemanticObjectsTypeEnum
from src.repository.aor import AorRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.database_object_relations import DatabaseObjectRelationsRepository
from src.repository.dimension import DimensionRepository
from src.repository.history.data_storage import DataStorageHistoryRepository
from src.repository.model import ModelRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.data_storage import DataStorageService
from tests.unit_tests.fixtures.data_storage import data_storage_model_list
from src.integration.worker_manager import ClientWorkerManager
from tests.unit_tests.mocks.aor_client import aor_client_mock
from tests.unit_tests.utils import clear_uncompared_fields


class TestWorkerManagerClient(ClientWorkerManager):
    def __init__(self) -> None:
        pass

    async def create_data_storage(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def update_data_storage(self, *args: Any, **kwargs: Any) -> None:
        return None


class TestDataStorageService:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model1", [], []),
            (
                "tenant1",
                "test_model1",
                data_storage_model_list[:12],
                "data_storages",
            ),
        ],
    )
    async def test_get_data_storage_list_by_model_name(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        model_name: str,
        expected: list[DataStorageModel],
        db_models: list[DataStorage],
        request: pytest.FixtureRequest,
        models: list[Model],
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        data_storages_res = await service.get_data_storage_list_by_model_name(tenant_id, model_name)
        data_storages_res_list = [DataStorageModel.model_dump(i, mode="json", by_alias=True) for i in data_storages_res]
        data_storages_res_list.sort(key=lambda data_storage: data_storage["name"])
        expected = copy.deepcopy(expected)
        expected_list = [DataStorageModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        expected_list.sort(key=lambda data_storage: data_storage["name"])
        assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(data_storages_res_list)

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "dso_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test_dso1", data_storage_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_data_storage_by_ds_name(
        self,
        mocked_session: AsyncSession,
        data_storages: list[DataStorage],
        tenant_id: str,
        model_name: str,
        dso_name: str,
        expected: DataStorageModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            data_storages_res = await service.get_data_storage_by_name(tenant_id, model_name, dso_name)
            expected = copy.deepcopy(expected)
            assert clear_uncompared_fields(expected.model_dump(mode="json", by_alias=True)) == clear_uncompared_fields(
                data_storages_res.model_dump(mode="json", by_alias=True)
            )

    @pytest.mark.parametrize(
        ("row", "expected_raise", "expected"),
        [
            (
                Series(
                    data={
                        DataStorageFileColumnEnum.NAME: "test1",
                        DataStorageFileColumnEnum.SHORT_LABEL: "short_label",
                        DataStorageFileColumnEnum.LONG_LABEL: "long_label",
                        DataStorageFileColumnEnum.PLAN: True,
                        DataStorageFileColumnEnum.TYPE: DataStorageEnum.CUBELIKE,
                    },
                ),
                does_not_raise(),
                DataStorageCreateRequestModel(
                    name="test1",
                    type=DataStorageEnum.CUBELIKE,
                    planning_enabled=True,
                    labels=[
                        Label(
                            language=Language.RU,
                            type=LabelType.SHORT,
                            text="short_label",
                        ),
                        Label(
                            language=Language.RU,
                            type=LabelType.LONG,
                            text="long_label",
                        ),
                    ],
                    fields=[
                        DataStorageFieldRequestModel(
                            name="empty",
                            ref_type=BaseFieldType(
                                ref_object="empty",
                                ref_object_type=BaseFieldTypeEnum.DIMENSION,
                            ),
                            semantic_type=SemanticType.DIMENSION,
                            sql_name="empty",
                        )
                    ],
                ),
            ),
            (
                Series(
                    data={
                        DataStorageFileColumnEnum.NAME: "test1",
                    },
                ),
                pytest.raises(Exception),
                DataStorageCreateRequestModel(
                    name="test1",
                    type=DataStorageEnum.CUBELIKE,
                    fields=[
                        DataStorageFieldRequestModel(
                            name="empty",
                            ref_type=BaseFieldType(
                                ref_object="empty",
                                ref_object_type=BaseFieldTypeEnum.DIMENSION,
                            ),
                            semantic_type=SemanticType.DIMENSION,
                            sql_name="empty",
                        )
                    ],
                ),
            ),
        ],
    )
    def test_convert_data_storage_df_row_to_pydantic(
        self,
        row: Series,
        mocked_session: AsyncSession,
        expected_raise: pytest.RaisesExc,
        expected: DataStorageCreateRequestModel,
    ) -> None:
        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            assert service.convert_data_storage_df_row_to_pydantic(row) == expected

    @pytest.mark.parametrize(
        ("row", "expected_raise", "expected"),
        [
            (
                Series(
                    data={
                        DataStorageFieldsFileColumnEnum.FIELD_NAME: "test1",
                        DataStorageFieldsFileColumnEnum.REF: "test2",
                        DataStorageFieldsFileColumnEnum.SEMANTIC_TYPE: SemanticType.DIMENSION,
                        DataStorageFieldsFileColumnEnum.KEY: True,
                        DataStorageFieldsFileColumnEnum.SHARDING_KEY: True,
                        DataStorageFieldsFileColumnEnum.SHORT_LABEL: "short_label",
                        DataStorageFieldsFileColumnEnum.LONG_LABEL: "long_label",
                    },
                ),
                does_not_raise(),
                DataStorageFieldRequestModel(
                    name="test1",
                    is_key=True,
                    is_sharding_key=True,
                    labels=[
                        Label(
                            language=Language.RU,
                            type=LabelType.SHORT,
                            text="short_label",
                        ),
                        Label(
                            language=Language.RU,
                            type=LabelType.LONG,
                            text="long_label",
                        ),
                    ],
                    ref_type=BaseFieldType(
                        ref_object="test2",
                        ref_object_type=BaseFieldTypeEnum.DIMENSION,
                    ),
                    semantic_type=SemanticType.DIMENSION,
                    sql_name="test1",
                ),
            ),
            (
                Series(
                    data={
                        DataStorageFieldsFileColumnEnum.FIELD_NAME: "test1",
                    },
                ),
                pytest.raises(Exception),
                DataStorageFieldRequestModel(
                    name="empty",
                    ref_type=BaseFieldType(
                        ref_object="empty",
                        ref_object_type=BaseFieldTypeEnum.DIMENSION,
                    ),
                    semantic_type=SemanticType.DIMENSION,
                    sql_name="empty",
                ),
            ),
        ],
    )
    def test_convert_data_storage_field_df_row_to_pydantic(
        self,
        mocked_session: AsyncSession,
        row: Series,
        expected_raise: pytest.RaisesExc,
        expected: DataStorageCreateRequestModel,
    ) -> None:
        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            assert service.convert_data_storage_field_df_row_to_pydantic(row) == expected

    @pytest.mark.parametrize(
        ("fields", "data_storages_for_create", "expected_exception", "expected"),
        [
            (
                {},
                {
                    "test1": DataStorageCreateRequestModel(
                        name="test1",
                        type=DataStorageEnum.CUBELIKE,
                        fields=[
                            DataStorageFieldRequestModel(
                                name="empty",
                                ref_type=BaseFieldType(
                                    ref_object="empty",
                                    ref_object_type=BaseFieldTypeEnum.DIMENSION,
                                ),
                                semantic_type=SemanticType.DIMENSION,
                                sql_name="empty",
                            )
                        ],
                    ),
                },
                ValueError,
                None,
            ),
            (
                {
                    "test1": [
                        DataStorageFieldRequestModel(
                            name="empty",
                            ref_type=BaseFieldType(
                                ref_object="empty",
                                ref_object_type=BaseFieldTypeEnum.DIMENSION,
                            ),
                            semantic_type=SemanticType.DIMENSION,
                            sql_name="empty",
                        )
                    ]
                },
                {
                    "test1": DataStorageCreateRequestModel(
                        name="test1",
                        type=DataStorageEnum.CUBELIKE,
                        fields=[
                            DataStorageFieldRequestModel(
                                name="empty",
                                ref_type=BaseFieldType(
                                    ref_object="empty",
                                    ref_object_type=BaseFieldTypeEnum.DIMENSION,
                                ),
                                semantic_type=SemanticType.DIMENSION,
                                sql_name="empty",
                            )
                        ],
                    ),
                },
                Exception,
                {"created": [], "not_created": ["test1"]},
            ),
        ],
    )
    async def test_try_create_data_storages_by_dict_for_create_and_fields(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        fields: dict[str, list[DataStorageFieldRequestModel]],
        data_storages_for_create: dict[str, DataStorageCreateRequestModel],
        expected_exception: Optional[Type[Exception]],
        expected: DataStorageCreateRequestModel,
    ) -> None:
        def mock_create_data_storage_by_schema(
            tenant_id: str, model_name: str, data_storage_model: DataStorageCreateRequestModel
        ) -> Any:
            if expected_exception:
                raise expected_exception("test")
            return None

        monkeypatch.setattr(DataStorageService, "create_data_storage_by_schema", mock_create_data_storage_by_schema)
        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        if expected_exception:
            with suppress(expected_exception):
                assert (
                    await service.try_create_data_storages_by_dict_for_create_and_fields(
                        "tenant1", "test_model1", fields, data_storages_for_create
                    )
                    == expected
                )
        else:
            assert (
                await service.try_create_data_storages_by_dict_for_create_and_fields(
                    "tenant1", "test_model1", fields, data_storages_for_create
                )
                == expected
            )

    @pytest.mark.parametrize(
        ("fields", "expected_exception", "expected"),
        [
            (
                {
                    "test1": [
                        DataStorageFieldRequestModel(
                            name="empty",
                            ref_type=BaseFieldType(
                                ref_object="empty",
                                ref_object_type=BaseFieldTypeEnum.DIMENSION,
                            ),
                            semantic_type=SemanticType.DIMENSION,
                            sql_name="empty",
                        )
                    ],
                },
                Exception,
                {"updated": [], "not_updated": ["test1"]},
            ),
        ],
    )
    async def test_try_update_data_storages_by_dict_fields_for_update(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        fields: dict[str, list[DataStorageFieldRequestModel]],
        expected_exception: Optional[Type[Exception]],
        expected: DataStorageCreateRequestModel,
    ) -> None:
        def mock_update_data_storage_by_name_and_schema(
            tenant_id: str,
            model_name: str,
            name: str,
            data_storage: DataStorageEditRequestModel,
        ) -> Any:
            if expected_exception:
                raise expected_exception("test")
            return None

        monkeypatch.setattr(
            DataStorageService, "create_data_storage_by_schema", mock_update_data_storage_by_name_and_schema
        )
        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        if expected_exception:
            with suppress(expected_exception):
                assert (
                    await service.try_update_data_storages_by_dict_fields_for_update("tenant1", "test_model1", fields)
                    == expected
                )
        else:
            assert (
                await service.try_update_data_storages_by_dict_fields_for_update("tenant1", "test_model1", fields)
                == expected
            )

    async def test_collect_views_for_model_creates_view_objects(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        data_storages: list[DataStorage],
        models: list[Model],
    ) -> None:
        """Проверяет создание VIEW-объектов и связей при сборе представлений."""

        calls: list[tuple[str, list[str]]] = []

        class FakeGenerator:
            async def find_views_by_table(
                self, database: Any, schema_name: str, table_names: list[str]
            ) -> list[dict[str, str]]:
                """Возвращает фиктивный список VIEW для проверки логики сбора."""
                calls.append((schema_name, table_names))
                return [
                    {
                        "view_schema": schema_name,
                        "view_name": "test_dso1_view",
                        "view_definition": "CREATE VIEW test_dso1_view AS SELECT * FROM test_schema1.test_dso1_distr",
                    }
                ]

        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()

        generator = FakeGenerator()
        parse_calls: dict[str, Any] = {}

        def fake_parse_view_ddl(ddl: str, view_name: str, dialect: Optional[str] = None) -> dict[str, Any]:
            """Возвращает минимальный JSON для VIEW и сохраняет параметры вызова."""
            parse_calls["ddl"] = ddl
            parse_calls["view_name"] = view_name
            parse_calls["dialect"] = dialect
            return {
                "type": "VIEW",
                "name": view_name,
                "columns": [],
                "dependencies": [{"type": "table", "name": "test_dso1_distr"}],
            }

        monkeypatch.setattr("src.service.data_storage.get_generator", lambda model: generator)
        monkeypatch.setattr("src.service.data_storage.parse_view_ddl", fake_parse_view_ddl)

        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        collected_ids = await service.collect_views_for_model(
            "tenant1", "test_model1", ["test_dso1"], send_to_aor=False
        )
        assert len(collected_ids) == 1
        assert parse_calls["dialect"] == "clickhouse"
        assert calls == [("test_schema1", ["test_dso1", "test_dso1_distr"])]

        db_object = (
            await mocked_session.execute(select(DatabaseObject).where(DatabaseObject.id == collected_ids[0]))
        ).scalars().one()
        assert db_object.name == "test_dso1_view"
        assert db_object.type == DbObjectTypeEnum.VIEW
        assert db_object.json_definition == {
            "type": "VIEW",
            "name": "test_dso1_view",
            "columns": [],
            "dependencies": [{"type": "table", "name": "test_dso1_distr"}],
        }

        relation = (
            await mocked_session.execute(
                select(DatabaseObjectRelation).where(DatabaseObjectRelation.database_object_id == db_object.id)
            )
        ).scalars().one()
        assert relation.semantic_object_type == "DATA_STORAGE"
        assert relation.database_object_id == db_object.id

    async def test_collect_views_for_model_skips_partial_table_name_matches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        data_storages: list[DataStorage],
        models: list[Model],
    ) -> None:
        """Проверяет, что VIEW не привязывается по частичному совпадению имени таблицы."""

        class FakeGenerator:
            async def find_views_by_table(
                self, database: Any, schema_name: str, table_names: list[str]
            ) -> list[dict[str, str]]:
                return [
                    {
                        "view_schema": schema_name,
                        "view_name": "test_dso1_shadow_view",
                        "view_definition": (
                            "CREATE VIEW test_dso1_shadow_view AS "
                            "SELECT * FROM test_schema1.test_dso11"
                        ),
                    }
                ]

        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()

        def fake_parse_view_ddl(ddl: str, view_name: str, dialect: Optional[str] = None) -> dict[str, Any]:
            return {
                "type": "VIEW",
                "name": view_name,
                "columns": [],
                "dependencies": [{"type": "table", "name": "test_dso11"}],
            }

        monkeypatch.setattr("src.service.data_storage.get_generator", lambda model: FakeGenerator())
        monkeypatch.setattr("src.service.data_storage.parse_view_ddl", fake_parse_view_ddl)

        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        collected_ids = await service.collect_views_for_model(
            "tenant1", "test_model1", ["test_dso1"], send_to_aor=False
        )
        assert collected_ids == []

        view_count = (
            await mocked_session.execute(
                select(func.count(DatabaseObject.id)).where(DatabaseObject.type == DbObjectTypeEnum.VIEW)
            )
        ).scalar_one()
        assert view_count == 0

    async def test_save_history_moves_outdated_view_relations_to_history(
        self,
        mocked_session: AsyncSession,
        data_storages: list[DataStorage],
        models: list[Model],
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()

        data_storage = (
            await mocked_session.execute(select(DataStorage).where(DataStorage.name == "test_dso1"))
        ).scalars().one()
        view = DatabaseObject(
            version=1,
            name="test_dso1_view",
            schema_name="test_schema1",
            tenant_id="tenant1",
            type=DbObjectTypeEnum.VIEW,
            specific_attributes=[],
        )
        mocked_session.add(view)
        await mocked_session.flush()

        relation = DatabaseObjectRelation(
            semantic_object_type=SemanticObjectsTypeEnum.DATA_STORAGE,
            semantic_object_id=data_storage.id,
            semantic_object_version=data_storage.version,
            database_object_id=view.id,
            database_object_version=view.version,
            relation_type=DatabaseObjectRelationTypeEnum.PARENT,
            version=1,
        )
        mocked_session.add(relation)
        await mocked_session.commit()

        history_repository = DataStorageHistoryRepository(mocked_session)
        await history_repository.save_history(data_storage, forced=True)

        active_relations = (
            await mocked_session.execute(select(DatabaseObjectRelation).where(DatabaseObjectRelation.id == relation.id))
        ).scalars().all()
        assert active_relations == []

        relation_history_model = DatabaseObjectRelation.__history_mapper__.class_  # type: ignore
        history_relation = (
            await mocked_session.execute(
                select(relation_history_model).where(
                    relation_history_model.id == relation.id,
                    relation_history_model.version == relation.version,
                )
            )
        ).scalars().one()
        assert history_relation.semantic_object_version == 1

        assert data_storage.version == 2

    async def test_collect_views_for_model_returns_empty_when_no_views(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocked_session: AsyncSession,
        data_storages: list[DataStorage],
        models: list[Model],
    ) -> None:
        """Проверяет, что при отсутствии VIEW возвращается пустой список."""

        class FakeGenerator:
            async def find_views_by_table(
                self, database: Any, schema_name: str, table_names: list[str]
            ) -> list[dict[str, str]]:
                """Возвращает пустой список, чтобы имитировать отсутствие VIEW."""
                return []

        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()

        def fake_parse_view_ddl(ddl: str, view_name: str, dialect: Optional[str] = None) -> dict[str, Any]:
            """Не должен вызываться при отсутствии VIEW."""
            raise AssertionError("parse_view_ddl should not be called when no views are found.")

        monkeypatch.setattr("src.service.data_storage.get_generator", lambda model: FakeGenerator())
        monkeypatch.setattr("src.service.data_storage.parse_view_ddl", fake_parse_view_ddl)

        service = DataStorageService(
            DataStorageRepository.get_by_session(mocked_session),
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            ModelRepository.get_by_session(mocked_session),
            DatabaseObjectRepository(mocked_session),
            DatabaseObjectRelationsRepository(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        collected_ids = await service.collect_views_for_model(
            "tenant1", "test_model1", ["test_dso1"], send_to_aor=False
        )
        assert collected_ids == []

        view_count = (
            await mocked_session.execute(
                select(func.count(DatabaseObject.id)).where(DatabaseObject.type == DbObjectTypeEnum.VIEW)
            )
        ).scalar_one()
        assert view_count == 0
