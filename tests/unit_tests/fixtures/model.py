from datetime import datetime
from uuid import UUID

import pytest

from src.db.database import Connection, Database, Port
from src.db.model import Model, ModelLabel
from src.models.database import (
    Connection as ConnectionModel,
    ConnetionTypeEnum,
    Database as DatabaseModel,
    DatabaseTypeEnum,
    Port as PortModel,
    ProtocolTypeEnum,
)
from src.models.label import Label, LabelType
from src.models.model import Model as ModelModel, ModelEditRequest as ModelEditRequestModel


@pytest.fixture
def models() -> list[Model]:
    return [
        Model(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            id=1,
            aor_space_id=UUID("0ce26657-4fa8-4196-af26-21a8125faec0"),
            name="test_model1",
            schema_name="test_schema1",
            tenant_id="tenant1",
            dimension_tech_fields=False,
            database=Database(
                version=1,
                timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
                name="test_database1",
                tenant_id="tenant1",
                default_cluster_name="test_cluster",
                type=DatabaseTypeEnum.CLICKHOUSE,
                db_name="ch1",
                connections=[
                    Connection(
                        host="127.0.0.1",
                        type=ConnetionTypeEnum.LOAD_BALANCER,
                        ports=[
                            Port(
                                port=5555,
                                protocol=ProtocolTypeEnum.CLICKHOUSE_HTTP,
                                sql_dialect=DatabaseTypeEnum.CLICKHOUSE,
                                secured=False,
                            ),
                        ],
                    ),
                ],
            ),
            labels=[
                ModelLabel(
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                ModelLabel(
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
        ),
        Model(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            id=2,
            aor_space_id=UUID("66e4152d-7701-45a0-96dd-0107a2c6dd6a"),
            name="test_model2",
            schema_name="test_schema2",
            dimension_tech_fields=False,
            tenant_id="tenant1",
            labels=[
                ModelLabel(
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                ModelLabel(
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
            database=Database(
                version=1,
                timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
                name="test_database3",
                tenant_id="tenant1",
                db_name="ch3",
                default_cluster_name="test_cluster",
                type=DatabaseTypeEnum.CLICKHOUSE,
                connections=[
                    Connection(
                        host="127.0.0.1",
                        type=ConnetionTypeEnum.LOAD_BALANCER,
                        ports=[
                            Port(
                                port=5555,
                                protocol=ProtocolTypeEnum.CLICKHOUSE_HTTP,
                                sql_dialect=DatabaseTypeEnum.CLICKHOUSE,
                                secured=False,
                            ),
                        ],
                    ),
                ],
            ),
        ),
        Model(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            id=3,
            aor_space_id=UUID("db1c6070-6c37-459b-a193-7ee89c97b1e6"),
            name="test_model3",
            tenant_id="tenant1",
            dimension_tech_fields=False,
            schema_name="test_schema3",
            labels=[
                ModelLabel(
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                ModelLabel(
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
            database=Database(
                version=1,
                timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
                name="test_database2",
                tenant_id="tenant1",
                db_name="gp2",
                type=DatabaseTypeEnum.GREENPLUM,
                connections=[
                    Connection(
                        host="127.0.0.1",
                        type=ConnetionTypeEnum.LOAD_BALANCER,
                        ports=[
                            Port(
                                port=1111,
                                protocol=ProtocolTypeEnum.POSTGRESQL_V3,
                                sql_dialect=DatabaseTypeEnum.GREENPLUM,
                                secured=False,
                            ),
                        ],
                    ),
                ],
            ),
        ),
    ]


model_model_list = [
    ModelModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test_model1",
        aor_space_id=UUID("0ce26657-4fa8-4196-af26-21a8125faec0"),
        schema_name="test_schema1",
        dimension_tech_fields=False,
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
        database=DatabaseModel(
            version=1,
            timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
            name="test_database1",
            db_name="ch1",
            tenant_id="tenant1",
            default_cluster_name="test_cluster",
            type=DatabaseTypeEnum.CLICKHOUSE,
            connections=[
                ConnectionModel(
                    host="127.0.0.1",
                    type=ConnetionTypeEnum.LOAD_BALANCER,
                    ports=[
                        PortModel(
                            port=1000,
                            protocol=ProtocolTypeEnum.CLICKHOUSE_HTTP,
                            sql_dialect=DatabaseTypeEnum.CLICKHOUSE,
                            secured=False,
                        ),
                    ],
                ),
            ],
        ),
        database_name="test_database1",
    ),
    ModelModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test_model2",
        aor_space_id=UUID("66e4152d-7701-45a0-96dd-0107a2c6dd6a"),
        schema_name="test_schema2",
        dimension_tech_fields=False,
        database_name="test_database3",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    ModelModel(
        version=1,
        timestamp=datetime(2023, 1, 1, 1, 1, 1, 1),
        name="test_model3",
        aor_space_id=UUID("db1c6070-6c37-459b-a193-7ee89c97b1e6"),
        schema_name="test_schema3",
        dimension_tech_fields=False,
        database_name="test_database2",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
]

model_model_update_list = [
    ModelEditRequestModel(
        database_id="test_database2",
        aor_space_id=UUID("d28444ea-77d4-4434-9151-7e6d5e818733"),
        dimension_tech_fields=False,
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    ModelEditRequestModel(
        database_id="test_database1",
        aor_space_id=UUID("7c9c54d0-bbcf-4acf-b75a-5e08089550d3"),
        dimension_tech_fields=False,
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
]
