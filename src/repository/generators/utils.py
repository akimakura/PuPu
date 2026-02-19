from typing import Type

from src.db.model import Model
from src.models.database import Database as DatabaseModel, DatabaseTypeEnum
from src.repository.generators.base_generator import GeneratorRepository
from src.repository.generators.clickhouse_generator import GeneratorClickhouseRepository
from src.repository.generators.postgresql_generator import GeneratorPostgreSQLRepository


def get_generator(model: Model) -> Type[GeneratorRepository]:
    """
    Возвращает один из генераторов в зависимости от типа базы данных:
     - GeneratorClickhouseRepository для ClickHouse
     - GeneratorPostgreSQLRepository для PostgreSQL или GreenPlum
    """
    database = DatabaseModel.model_validate(model.database)
    if database.type == DatabaseTypeEnum.CLICKHOUSE:
        return GeneratorClickhouseRepository
    if database.type in (DatabaseTypeEnum.POSTGRESQL, DatabaseTypeEnum.GREENPLUM):
        return GeneratorPostgreSQLRepository
    raise ValueError("Unknown generator type")
