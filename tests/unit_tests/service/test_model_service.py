from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.model import Model
from src.models.model import Model as ModelModel
from src.repository.aor import AorRepository
from src.repository.meta_synchronizer import MetaSynchronizerRepository
from src.repository.model import ModelRepository
from src.service.model import ModelService
from tests.unit_tests.fixtures.model import model_model_list
from tests.unit_tests.mocks.aor_client import aor_client_mock
from tests.unit_tests.utils import clear_uncompared_fields


class TestModelService:

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "db_models"),
        [
            ("tenant1", [], []),
            (
                "tenant1",
                model_model_list,
                "models",
            ),
        ],
    )
    async def test_get_model_list(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        expected: list[ModelModel],
        db_models: list[Model],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()

        service = ModelService(
            ModelRepository.get_by_session(mocked_session),
            MetaSynchronizerRepository.get_by_session(mocked_session),
            aor_client_mock,
            AorRepository(mocked_session),
        )
        model_res = await service.get_model_list(tenant_id)
        expected_list = [ModelModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        model_res_list = [ModelModel.model_dump(i, mode="json", by_alias=True) for i in model_res]
        assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(model_res_list)

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", model_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_model_by_name(
        self,
        mocked_session: AsyncSession,
        models: list[Model],
        tenant_id: str,
        model_name: str,
        expected: ModelModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()

        service = ModelService(
            ModelRepository.get_by_session(mocked_session),
            MetaSynchronizerRepository.get_by_session(mocked_session),
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            model_res = await service.get_model_by_name(tenant_id, model_name)
            assert clear_uncompared_fields(expected.model_dump(mode="json", by_alias=True)) == clear_uncompared_fields(
                model_res.model_dump(mode="json", by_alias=True)
            )
