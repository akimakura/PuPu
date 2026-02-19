import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.dimension import Dimension
from src.repository.composite import CompositeRepository
from src.repository.data_storage import DataStorageRepository
from src.repository.database_object import DatabaseObjectRepository
from src.repository.dimension import DimensionRepository
from src.repository.measure import MeasureRepository
from src.repository.model import ModelRepository
from src.repository.model_relations import ModelRelationsRepository


class TestModelRelationsRepository:

    @pytest.mark.parametrize(
        ("dimension_name", "expected"),
        [
            ("test_dim5", True),
            ("test_dim7", False),
        ],
    )
    async def test_check_if_dimension_can_be_deleted(
        self,
        mocked_session: AsyncSession,
        dimensions: list[Dimension],
        dimension_name: str,
        expected: bool,
    ) -> None:
        mocked_session.add_all(dimensions)
        await mocked_session.commit()
        dim_repository = DimensionRepository.get_by_session(mocked_session)
        dimension = await dim_repository.get_dimension_orm_model("tenant1", dimension_name, "test_model1")
        model_repository = ModelRepository.get_by_session(mocked_session)
        datastorage_repository = DataStorageRepository.get_by_session(mocked_session)
        database_object_repository = DatabaseObjectRepository(mocked_session)
        measure_repository = MeasureRepository.get_by_session(mocked_session)
        composite_repository = CompositeRepository(
            mocked_session, model_repository, datastorage_repository, database_object_repository
        )
        model_relations_repository = ModelRelationsRepository(
            session=mocked_session,
            measure_repository=measure_repository,
            composite_repository=composite_repository,
            model_repository=model_repository,
            datastorage_repository=datastorage_repository,
        )
        possible_delete, _ = await model_relations_repository.check_if_dimension_can_be_deleted(
            dimension.id, dimensions[0].models[0].id
        )
        assert expected == possible_delete
