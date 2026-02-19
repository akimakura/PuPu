from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.model import Model
from src.models.model import Model as ModelModel
from src.repository.model import ModelRepository
from tests.unit_tests.fixtures.model import model_model_list


class TestModelRepository:

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
    async def test_get_list(
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
        repository = ModelRepository.get_by_session(mocked_session)
        model_res = await repository.get_list(tenant_id)
        expected_list = [ModelModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        model_res_list = [ModelModel.model_dump(i, mode="json", by_alias=True) for i in model_res]
        assert expected_list == model_res_list

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", model_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_by_name(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        models: list[Model],
        model_name: str,
        expected: ModelModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()
        repository = ModelRepository.get_by_session(mocked_session)
        with expected_raise:
            model_res = await repository.get_by_name(tenant_id, model_name)
            assert expected.model_dump(mode="json", by_alias=True) == model_res.model_dump(mode="json", by_alias=True)
