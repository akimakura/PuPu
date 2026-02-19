from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.data_storage import DataStorage
from src.db.model import Model
from src.models.data_storage import DataStorage as DataStorageModel
from src.repository.data_storage import DataStorageRepository
from tests.unit_tests.fixtures.data_storage import data_storage_model_list


class TestDataStorageRepository:

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
    async def test_get_list(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        model_name: str,
        expected: list[DataStorageModel],
        db_models: list[DataStorage],
        models: list[Model],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        repository = DataStorageRepository.get_by_session(mocked_session)
        data_storages_res = await repository.get_list(tenant_id, model_name)
        data_storages_res_list = [DataStorageModel.model_dump(i, mode="json", by_alias=True) for i in data_storages_res]
        expected_list = [DataStorageModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        expected_list.sort(key=lambda data_storage: data_storage["name"])
        data_storages_res_list.sort(key=lambda data_storage: data_storage["name"])
        assert expected_list == data_storages_res_list

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "dso_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test_dso1", data_storage_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_by_name(
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
        repository = DataStorageRepository.get_by_session(mocked_session)
        with expected_raise:
            data_storage_res = await repository.get_by_name(tenant_id=tenant_id, name=dso_name, model_name=model_name)
            assert expected.model_dump(mode="json", by_alias=True) == data_storage_res.model_dump(
                mode="json", by_alias=True
            )
