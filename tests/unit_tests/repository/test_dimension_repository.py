from collections import namedtuple
from contextlib import nullcontext as does_not_raise
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.dimension import Dimension
from src.models.data_storage import DataStorageLogsFieldEnum
from src.models.dimension import (
    Dimension as DimensionModel,
    DimensionCreateRequest as DimensionCreateRequestModel,
    DimensionTypeEnum,
    TextEnum,
)
from src.repository.dimension import DimensionRepository
from tests.unit_tests.fixtures.dimension import dimension_model_list

Model_test = namedtuple("Model_test", ["name"])
Dimension_test = namedtuple("Dimension_test", ["name", "models_names", "models"])


class TestDimensionRepository:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "names", "expected", "db_models"),
        [
            ("tenant1", "test_model1", None, [], []),
            (
                "tenant1",
                "test_model1",
                None,
                dimension_model_list[:8],
                "dimensions",
            ),
            (
                "tenant1",
                "test_model1",
                [
                    "test_dim5",
                ],
                [dimension_model_list[0]],
                "dimensions",
            ),
        ],
    )
    async def test_get_list(
        self,
        mocked_session: AsyncSession,
        model_name: str,
        tenant_id: str,
        names: Optional[list[str]],
        expected: list[DimensionModel],
        db_models: list[Dimension],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        repository = DimensionRepository.get_by_session(mocked_session)
        dimension_res = await repository.get_list(tenant_id, model_name, names)
        expected_list = [DimensionModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        expected_list.sort(key=lambda dimension: dimension["name"])
        dimension_res_list = [DimensionModel.model_dump(i, mode="json", by_alias=True) for i in dimension_res]
        dimension_res_list.sort(key=lambda dimension: dimension["name"])
        assert expected_list == dimension_res_list

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test_dim5555", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test_dim5", dimension_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_by_name(
        self,
        mocked_session: AsyncSession,
        dimensions: list[Dimension],
        tenant_id: str,
        model_name: str,
        name: str,
        expected: DimensionModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        repository = DimensionRepository.get_by_session(mocked_session)
        with expected_raise:
            dimension = await repository.get_by_name(tenant_id, name, model_name)
            assert expected.model_dump(mode="json", by_alias=True) == dimension.model_dump(mode="json", by_alias=True)

    @pytest.mark.parametrize(
        (
            "tenant_id",
            "model_names",
            "mock_get_dimension_result",
            "is_dimension_exist",
            "generate_on_db",
            "expected_raise",
        ),
        [
            ("tenant1", ["model1", "model2"], None, False, True, does_not_raise()),
            ("tenant1", [], None, True, True, pytest.raises(ValueError)),
            (
                "tenant1",
                ["new_model"],
                Dimension_test(
                    name=DataStorageLogsFieldEnum.OPERATION.value,
                    models_names=[Model_test(name="existing_model")],
                    models=None,
                ),
                True,
                True,
                does_not_raise(),
            ),
            (
                "tenant1",
                ["another_new_model"],
                MagicMock(models=[Model_test(name="existing_model")]),
                True,
                True,
                does_not_raise(),
            ),
        ],
    )
    async def test_create_not_virtual_dimensions(
        self,
        mocked_session: AsyncSession,
        async_client: AsyncClient,
        tenant_id: str,
        model_names: list[str],
        mock_get_dimension_result: Any,
        is_dimension_exist: bool,
        generate_on_db: bool,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        """
        Тестирование функции создания физического operation

        Параметры:
            mocked_session (AsyncSession): Мокированная сессия базы данных.
            tenant_id (str): ID клиента.
            model_names (list[str]): Список имен моделей.
            is_dimension_exist (bool): Существует ли уже operation
            generate_on_db (bool): Флаг генерации на стороне базы данных.
            expected_raise (pytest.RaisesExc): Контекст обработчика исключения.

        Возвращаемое значение:
            None
        """
        repository = DimensionRepository.get_by_session(mocked_session)
        repository.get_dimension_orm_model = AsyncMock(return_value=mock_get_dimension_result)  # type: ignore
        create_by_schema_mock = AsyncMock()
        copy_model_dimension_mock = AsyncMock()
        commit_mock = AsyncMock()
        with patch.object(
            repository, "create_by_schema", new=create_by_schema_mock
        ) as mock_create_by_schema, patch.object(
            repository, "copy_model_dimension", new=copy_model_dimension_mock
        ) as mock_copy_model_dimension, patch.object(
            mocked_session, "commit", new=commit_mock
        ), expected_raise:
            await repository.create_not_virtual_dimensions(
                tenant_id=tenant_id, model_names=model_names, generate_on_db=generate_on_db
            )
            if is_dimension_exist and model_names:
                for model in model_names:
                    if model not in model_names:
                        mock_copy_model_dimension.assert_any_call(
                            tenant_id=tenant_id,
                            name=DataStorageLogsFieldEnum.OPERATION,
                            model_names=[model],
                            copy_attributes=True,
                            generate_on_db=True,
                            if_not_exists=True,
                        )
            elif not is_dimension_exist and model_names:
                mock_create_by_schema.assert_called_once_with(
                    tenant_id=tenant_id,
                    model_name=model_names[0],
                    dimension=DimensionCreateRequestModel(
                        name=DataStorageLogsFieldEnum.OPERATION.value,
                        texts=[TextEnum.LONG],
                        type=DimensionTypeEnum.STRING,
                        precision=255,
                    ),
                    if_not_exists=True,
                    generate_on_db=True,
                )
