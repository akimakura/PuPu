from contextlib import nullcontext as does_not_raise
from typing import Any

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.composite import Composite
from src.db.data_storage import DataStorage
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.models.composite import Composite as CompositeModel
from src.repository.aor import AorRepository
from src.repository.composite import CompositeRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.composite import CompositeService
from tests.unit_tests.fixtures.composite import composite_model_list
from tests.unit_tests.mocks.aor_client import aor_client_mock
from tests.unit_tests.utils import clear_uncompared_fields


class TestWorkerManagerClient:

    def create_composite(self, *args: Any, **kwargs: Any) -> None:
        pass

    def update_composite(self, *args: Any, **kwargs: Any) -> None:
        pass


class TestCompositeService:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model1", [], []),
            (
                "tenant1",
                "test_model1",
                composite_model_list[:2],
                "composites",
            ),
        ],
    )
    async def test_get_composite_list_by_model_name(
        self,
        mocked_session: AsyncSession,
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        expected: list[CompositeModel],
        db_models: list[Composite],
        request: pytest.FixtureRequest,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        service = CompositeService(
            CompositeRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        composites_res = await service.get_composite_list_by_model_name(tenant_id, model_name)
        expected_list = [CompositeModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        composites_res_list = [CompositeModel.model_dump(i, mode="json", by_alias=True) for i in composites_res]
        assert clear_uncompared_fields(composites_res_list) == clear_uncompared_fields(expected_list)

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "composite_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test_composite1", composite_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_composite_by_name(
        self,
        mocked_session: AsyncSession,
        composites: list[Composite],
        measures: list[Measure],
        dimensions: list[Dimension],
        data_storages: list[DataStorage],
        models: list[Model],
        tenant_id: str,
        model_name: str,
        composite_name: str,
        expected: CompositeModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        mocked_session.add_all(measures)
        await mocked_session.commit()
        mocked_session.add_all(data_storages)
        await mocked_session.commit()
        mocked_session.add_all(composites)
        await mocked_session.commit()
        service = CompositeService(
            CompositeRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            composite = await service.get_composite_by_name(
                tenant_id=tenant_id, name=composite_name, model_name=model_name
            )
            composite_res = CompositeModel.model_dump(composite, mode="json", by_alias=True)
            expected_res = CompositeModel.model_dump(expected, mode="json", by_alias=True)
            assert clear_uncompared_fields(composite_res) == clear_uncompared_fields(expected_res)
