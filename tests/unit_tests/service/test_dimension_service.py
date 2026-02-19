from contextlib import nullcontext as does_not_raise
from io import BytesIO
from typing import Any

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.dimension import Dimension
from src.db.model import Model
from src.models.dimension import Dimension as DimensionModel
from src.models.model_import import ImportFromFileResponse
from src.repository.aor import AorRepository
from src.repository.dimension import DimensionRepository
from src.repository.model_relations import ModelRelationsRepository
from src.service.dimension import DimensionService
from tests.unit_tests.fixtures.dimension import dimension_model_list
from tests.unit_tests.mocks.aor_client import aor_client_mock
from tests.unit_tests.utils import clear_uncompared_fields


class MockedUploadFile:

    def __init__(self, filepath: str) -> None:
        with open(filepath, "rb") as file:
            self.file = BytesIO(file.read())
        self.content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class TestWorkerManagerClient:

    def create_dimension(self, *args: Any, **kwargs: Any) -> None:
        pass

    def update_dimension(self, *args: Any, **kwargs: Any) -> None:
        pass


class TestDimensionService:

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "expected", "db_models"),
        [
            ("tenant1", "test_model1", [], []),
            (
                "tenant1",
                "test_model1",
                dimension_model_list[:8],
                "dimensions",
            ),
        ],
    )
    async def test_get_dimension_list_by_model_name(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        model_name: str,
        expected: list[DimensionModel],
        db_models: list[Dimension],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()

        service = DimensionService(
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        dimension_res = await service.get_dimension_list_by_model_name(tenant_id, model_name)
        expected_list = [DimensionModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        expected_list.sort(key=lambda dimension: dimension["name"])
        dimension_res_list = [DimensionModel.model_dump(i, mode="json", by_alias=True) for i in dimension_res]
        dimension_res_list.sort(key=lambda dimension: dimension["name"])
        assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(dimension_res_list)

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "names", "expected", "db_models"),
        [
            (
                "tenant1",
                "test_model1",
                [
                    "test_dim5",
                ],
                [dimension_model_list[0]],
                "dimensions",
            ),
            (
                "tenant1",
                "test_model1",
                [],
                [],
                "dimensions",
            ),
        ],
    )
    async def test_get_dimension_list_by_names(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        model_name: str,
        names: list[str],
        expected: list[DimensionModel],
        db_models: list[Dimension],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()

        service = DimensionService(
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        dimension_res = await service.get_dimension_list_by_names(tenant_id, model_name, names=names)
        expected_list = [DimensionModel.model_dump(i, mode="json", by_alias=True) for i in expected]
        dimension_res_list = [DimensionModel.model_dump(i, mode="json", by_alias=True) for i in dimension_res]
        assert clear_uncompared_fields(expected_list) == clear_uncompared_fields(dimension_res_list)

    @pytest.mark.parametrize(
        ("tenant_id", "model_name", "name", "expected", "expected_raise"),
        [
            ("tenant1", "test_model1", "test_dim5555", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_model1", "test_dim5", dimension_model_list[0], does_not_raise()),
        ],
    )
    async def test_get_dimension_by_dimension_name(
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

        service = DimensionService(
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        with expected_raise:
            dimension = await service.get_dimension_by_dimension_name(tenant_id, name, model_name)
            assert clear_uncompared_fields(expected.model_dump(mode="json", by_alias=True)) == clear_uncompared_fields(
                dimension.model_dump(mode="json", by_alias=True)
            )

    async def test_create_dimensions_by_files(
        self,
        mocked_session: AsyncSession,
        models: list[Model],
    ) -> None:
        mocked_session.add_all(models)
        await mocked_session.commit()

        file_dimensions = MockedUploadFile("tests/unit_tests/fixtures/data_xlsx/test_import_dimension.xlsx")
        file_attributes = MockedUploadFile("tests/unit_tests/fixtures/data_xlsx/test_import_dimension_attrs.xlsx")
        service = DimensionService(
            DimensionRepository.get_by_session(mocked_session),
            ModelRelationsRepository.get_by_session(mocked_session),
            TestWorkerManagerClient(),  # type: ignore
            aor_client_mock,
            AorRepository(mocked_session),
        )
        result = await service.create_dimensions_by_files("tenant1", "test_model1", file_dimensions, file_attributes)  # type: ignore
        result.created.sort()
        result.updated.sort()
        assert result == ImportFromFileResponse(
            created=["unit_test1", "unit_test2", "unit_test3", "unit_test4"],
            updated=["unit_test1", "unit_test2", "unit_test3"],
        )
