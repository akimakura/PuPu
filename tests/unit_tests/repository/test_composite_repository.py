from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.composite import Composite
from src.db.data_storage import DataStorage
from src.db.dimension import Dimension
from src.db.measure import Measure
from src.db.model import Model
from src.models.composite import Composite as CompositeModel
from src.repository.composite import CompositeRepository
from tests.unit_tests.fixtures.composite import composite_model_list


class TestCompositeRepository:

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
    async def test_get_list(
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
        database_objects: list[list] = [models, dimensions, measures, data_storages]
        for objects in database_objects:
            mocked_session.add_all(objects)
            await mocked_session.commit()
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        repository = CompositeRepository.get_by_session(mocked_session)
        composites_res = await repository.get_list(tenant_id, model_name)
        expected_list = [CompositeModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        composites_res_list = [CompositeModel.model_dump(i, mode="json", by_alias=True) for i in composites_res]
        assert composites_res_list == expected_list

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "composite_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test_composite1", composite_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_by_name(
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
        database_objects: list[list] = [models, dimensions, measures, data_storages, composites]
        for objects in database_objects:
            mocked_session.add_all(objects)
            await mocked_session.commit()
        repository = CompositeRepository.get_by_session(mocked_session)
        with expected_raise:
            composite = await repository.get_by_name(tenant_id=tenant_id, name=composite_name, model_name=model_name)
            composite_res = CompositeModel.model_dump(composite, mode="json", by_alias=True)
            expected_res = CompositeModel.model_dump(expected, mode="json", by_alias=True)
            assert composite_res == expected_res
