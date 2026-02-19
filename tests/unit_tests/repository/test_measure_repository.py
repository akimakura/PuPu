"""
Тут содержатся все тесты MeasureRepository.
"""

from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.measure import Measure
from src.models.measure import Measure as MeasureModel
from src.repository.measure import MeasureRepository
from tests.unit_tests.fixtures.measure import measure_model_list


class TestMeasureRepository:

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "db_models", "model_name"),
        [
            ("tenant1", [], [], "test_model3"),
            ("tenant1", measure_model_list[:2], "measures", "test_model1"),
        ],
    )
    async def test_get_list(
        self,
        mocked_session: AsyncSession,
        expected: list[MeasureModel],
        db_models: list[Measure],
        tenant_id: str,
        model_name: str,
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        repository = MeasureRepository.get_by_session(mocked_session)
        measures = await repository.get_list(tenant_id, model_name)
        assert expected == measures

    @pytest.mark.parametrize(
        ("tenant_id", "model_id", "measure_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test1", measure_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_by_name(
        self,
        mocked_session: AsyncSession,
        measures: list[Measure],
        model_id: str,
        tenant_id: str,
        measure_name: str,
        expected: MeasureModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(measures)
        await mocked_session.commit()
        repository = MeasureRepository.get_by_session(mocked_session)
        with expected_raise:
            measure = await repository.get_by_name(tenant_id=tenant_id, name=measure_name, model_name=model_id)
            assert expected == measure
