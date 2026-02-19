from contextlib import nullcontext as does_not_raise

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database import Database
from src.models.database import Database as DatabaseModel
from src.repository.database import DatabaseRepository
from tests.unit_tests.fixtures.database import database_model_list


class TestDatabaseRepository:

    @pytest.mark.parametrize(
        ("tenant_id", "expected", "db_models"),
        [
            ("tenant1", [], []),
            (
                "tenant1",
                database_model_list,
                "databases",
            ),
        ],
    )
    async def test_get_list(
        self,
        mocked_session: AsyncSession,
        tenant_id: str,
        expected: list[DatabaseModel],
        db_models: list[Database],
        request: pytest.FixtureRequest,
    ) -> None:
        if isinstance(db_models, str):
            db_models = request.getfixturevalue(db_models)
        mocked_session.add_all(db_models)
        await mocked_session.commit()
        repository = DatabaseRepository(mocked_session)
        database_res = await repository.get_list(tenant_id=tenant_id)
        assert expected == database_res

    @pytest.mark.parametrize(
        ("tenant_id", "database_name", "expected", "expected_raise"),
        [
            ("tenant1", "test_database1222", None, pytest.raises(NoResultFound)),
            ("tenant1", "test_database2", database_model_list[1], does_not_raise()),
        ],
    )
    async def test_get_by_name(
        self,
        mocked_session: AsyncSession,
        databases: list[Database],
        tenant_id: str,
        database_name: str,
        expected: DatabaseModel,
        expected_raise: pytest.RaisesExc,
    ) -> None:
        mocked_session.add_all(databases)
        await mocked_session.commit()
        repository = DatabaseRepository(mocked_session)
        with expected_raise:
            database_res = await repository.get_by_name(tenant_id, database_name)
            assert expected == database_res
