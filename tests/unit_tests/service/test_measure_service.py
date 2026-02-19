"""
Тут содержатся все тесты MeasureRepository.
"""

from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.measure import Measure
from src.models.measure import Measure as MeasureModel
from src.repository.aor import AorRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.measure import MeasureRepository, MeasureService
from tests.unit_tests.fixtures.measure import measure_model_list
from tests.unit_tests.mocks.aor_client import aor_client_mock
from tests.unit_tests.utils import clear_uncompared_fields


class TestMeasureService:

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "db_models", "model_name"),
        [
            ("tenant1", [], [], "model_name"),
            (
                "tenant1",
                measure_model_list[0:2],
                "measures",
                "test_model1",
            ),
        ],
    )
    async def test_get_measure_list_by_model_name(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        expected: list[MeasureModel],
        db_models: list[Measure],
        model_name: str,
        request: pytest.FixtureRequest,
    ) -> None:

        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()

        service = MeasureService(
            MeasureRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            aor_client_mock,
            AorRepository(mocked_session),
        )
        measures = await service.get_measure_list_by_model_name(tenant_id, model_name)
        assert clear_uncompared_fields(
            [value.model_dump(mode="json", by_alias=True) for value in expected]
        ) == clear_uncompared_fields([value.model_dump(mode="json", by_alias=True) for value in measures])

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "measure_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test1", measure_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_measure_by_measure_name(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        measures: list[Measure],
        model_name: str,
        measure_name: str,
        expected: MeasureModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(measures)
        await mocked_session.commit()

        service = MeasureService(
            MeasureRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            measure = await service.get_measure_by_measure_name(tenant_id, measure_name, model_name)
            assert clear_uncompared_fields(expected.model_dump(mode="json", by_alias=True)) == clear_uncompared_fields(
                measure.model_dump(mode="json", by_alias=True)
            )
