import pytest

from src.models.composite import CompositeFieldRefObjectEnum, CompositeLinkTypeEnum
from src.models.database import DatabaseTypeEnum
from src.models.database_object import DatabaseObject, DbObjectTypeEnum
from tests.unit_tests.fixtures.model import model_model_list


class DatasourceFieldMock:

    def __init__(self, sql_name: str) -> None:
        self.sql_name = sql_name


class datasourceMock:

    def __init__(self, database_objects: list[DatabaseObject]) -> None:
        self.database_objects = database_objects


datasources: list[dict] = [
    {
        "name": "test_datasource1",
        "type": CompositeFieldRefObjectEnum.COMPOSITE,
        "source": datasourceMock(
            database_objects=[
                DatabaseObject(
                    schema_name="test_schema1",
                    name="test_dbo1",
                    models=[model_model_list[0]],
                    type=DbObjectTypeEnum.VIEW,
                ),
            ]
        ),
    },
    {
        "name": "test_datasource2",
        "type": CompositeFieldRefObjectEnum.DATASTORAGE,
        "source": datasourceMock(
            database_objects=[
                DatabaseObject(
                    schema_name="test_schema2",
                    name="test_distr2",
                    models=[model_model_list[0]],
                    type=DbObjectTypeEnum.DISTRIBUTED_TABLE,
                ),
            ]
        ),
    },
    {
        "name": "test_datasource3",
        "type": CompositeFieldRefObjectEnum.DATASTORAGE,
        "source": datasourceMock(
            database_objects=[
                DatabaseObject(
                    schema_name="test_schema3",
                    name="test_dict3",
                    models=[model_model_list[0]],
                    type=DbObjectTypeEnum.DICTIONARY,
                ),
            ]
        ),
    },
    {
        "name": "test_datasource4",
        "type": CompositeFieldRefObjectEnum.VIEW,
        "schema_name": "test_schema4",
    },
    {
        "name": "test_datasource1",
        "type": CompositeFieldRefObjectEnum.DATASTORAGE,
        "source": datasourceMock(
            database_objects=[
                DatabaseObject(
                    schema_name="test_schema1",
                    name="test_distr1",
                    models=[model_model_list[0]],
                    type=DbObjectTypeEnum.DISTRIBUTED_TABLE,
                ),
            ]
        ),
    },
]


@pytest.fixture
def cases_for_generate_field_name() -> list[dict]:
    return [
        {
            "schema_name": "test_schema",
            "datasource_name": "test_datasource",
            "datasources_info_dict": {
                "test_datasource": {
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {"test_field": DatasourceFieldMock("test_field")},
                }
            },
            "field": {
                "sql_name": "test1",
                "name": "test1",
                "datasource_links": [
                    {
                        "datasource": "test_datasource",
                        "datasource_field": "test_field",
                    },
                ],
            },
            "model": model_model_list[0],
            "result": "`test_schema`.`test_datasource`.`test_field` as `test1`",
        },
        {
            "schema_name": "test_schema1",
            "datasource_name": "test_datasource1",
            "datasources_info_dict": {
                "test_datasource1": {
                    "type": CompositeFieldRefObjectEnum.VIEW,
                    "fields": {"test_field1": DatasourceFieldMock("test_field1")},
                }
            },
            "field": {
                "name": "test2",
                "datasource_links": [
                    {
                        "datasource": "test_datasource1",
                        "datasource_field": "test_field1",
                    },
                ],
            },
            "model": model_model_list[0],
            "result": "`test_schema1`.`test_datasource1`.`test_field1` as `test2`",
        },
    ]


@pytest.fixture
def cases_get_datasource_schema_name_and_datasource_name() -> list[dict]:
    return [
        {
            "datasource": datasources[0],
            "model": model_model_list[0],
            "result": ("test_schema1", "test_dbo1"),
        },
        {
            "datasource": datasources[1],
            "model": model_model_list[0],
            "result": ("test_schema2", "test_distr2"),
        },
        {
            "datasource": datasources[2],
            "model": model_model_list[0],
            "result": ("test_schema3", "test_dict3"),
        },
        {
            "datasource": datasources[3],
            "model": model_model_list[0],
            "result": ("test_schema4", "test_datasource4"),
        },
        {
            "datasource": {
                "type": "test_raise",
            },
            "model": model_model_list[0],
            "result": "",
        },
    ]


@pytest.fixture
def cases_for_generate_composite_union_sql_expression() -> list[dict]:
    return [
        {
            "datasources": [
                datasources[0],
                datasources[1],
            ],
            "datasources_info_dict": {
                "test_datasource1": {
                    "source": datasources[0]["source"],
                    "type": CompositeFieldRefObjectEnum.COMPOSITE,
                    "fields": {"test_field1": DatasourceFieldMock("test_field1_sql")},
                },
                "test_datasource2": {
                    "source": datasources[1]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {"test_field2": DatasourceFieldMock("test_field_sql")},
                },
            },
            "fields": [
                {
                    "name": "test_composite_field",
                    "sql_name": "test_composite_field_sql",
                    "datasource_links": [
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        },
                        {
                            "datasource": "test_datasource2",
                            "datasource_field": "test_field2",
                        },
                    ],
                }
            ],
            "model": model_model_list[0],
            "result": "SELECT `test_schema1`.`test_dbo1`.`test_field1_sql` as `test_composite_field_sql` FROM `test_schema1`.`test_dbo1` "
            + "UNION ALL SELECT `test_schema2`.`test_distr2`.`test_field_sql`"
            + " as `test_composite_field_sql` FROM `test_schema2`.`test_distr2`",
        },
        {
            "datasources": [
                datasources[0],
                datasources[1],
            ],
            "datasources_info_dict": {
                "test_datasource1": {
                    "source": datasources[0]["source"],
                    "type": CompositeFieldRefObjectEnum.COMPOSITE,
                    "fields": {"test_field1": DatasourceFieldMock("test_field1_sql")},
                },
                "test_datasource2": {
                    "source": datasources[1]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {"test_field2": DatasourceFieldMock("test_field_sql")},
                },
            },
            "fields": [
                {
                    "name": "test_composite_field",
                    "sql_name": "test_composite_field_sql",
                    "datasource_links": [
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        }
                    ],
                }
            ],
            "model": model_model_list[0],
            "result": "SELECT `test_schema1`.`test_dbo1`.`test_field1_sql` as `test_composite_field_sql`"
            + " FROM `test_schema1`.`test_dbo1` UNION ALL SELECT NULL as test_composite_field_sql FROM `test_schema2`.`test_distr2`",
        },
        {
            "datasources": [
                datasources[0],
                datasources[1],
            ],
            "datasources_info_dict": {
                "test_datasource1": {
                    "source": datasources[0]["source"],
                    "type": CompositeFieldRefObjectEnum.COMPOSITE,
                    "fields": {"test_field1": DatasourceFieldMock("test_field1_sql")},
                },
                "test_datasource2": {
                    "source": datasources[1]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {"test_field2": DatasourceFieldMock("test_field_sql")},
                },
            },
            "fields": [
                {
                    "name": "test_composite_field",
                    "sql_name": "test_composite_field_sql",
                    "datasource_links": [
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        },
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        },
                    ],
                }
            ],
            "model": model_model_list[0],
            "result": "SELECT `test_schema1`.`test_dbo1`.`test_field1_sql` as `test_composite_field_sql` FROM `test_schema1`.`test_dbo1` "
            + "UNION ALL SELECT NULL as test_composite_field_sql FROM `test_schema2`.`test_distr2`",
        },
    ]


@pytest.fixture
def cases_generate_composite_join_sql_expression() -> list[dict]:
    return [
        {
            "link_type": CompositeLinkTypeEnum.INNER_JOIN,
            "datasources": [
                datasources[4],
                datasources[1],
            ],
            "links_fields": [
                {
                    "left": {
                        "datasource": "test_datasource1",
                        "datasource_field": "test_field1",
                    },
                    "right": {
                        "datasource": "test_datasource2",
                        "datasource_field": "test_field2",
                    },
                },
                {
                    "left": {
                        "datasource": "test_datasource1",
                        "datasource_field": "test_field3",
                    },
                    "right": {
                        "datasource": "test_datasource2",
                        "datasource_field": "test_field4",
                    },
                },
            ],
            "datasources_info_dict": {
                "test_datasource1": {
                    "source": datasources[4]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {
                        "test_field1": DatasourceFieldMock("test_field1_sql"),
                        "test_field3": DatasourceFieldMock("test_field3_sql"),
                    },
                },
                "test_datasource2": {
                    "source": datasources[1]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {
                        "test_field2": DatasourceFieldMock("test_field_sql"),
                        "test_field4": DatasourceFieldMock("test_field4_sql"),
                    },
                },
            },
            "fields": [
                {
                    "name": "test_composite_field",
                    "sql_name": "test_composite_field_sql",
                    "datasource_links": [
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        },
                    ],
                }
            ],
            "model": model_model_list[0],
            "result": "SELECT `test_schema1`.`test_distr1`.`test_field1_sql` as "
            + "`test_composite_field_sql` FROM `test_schema1`.`test_distr1` GLOBAL INNER JOIN `test_schema2`.`test_distr2`"
            + " ON `test_schema1`.`test_distr1`.`test_field1_sql` = `test_schema2`.`test_distr2`.`test_field_sql`"
            + " AND `test_schema1`.`test_distr1`.`test_field3_sql` = `test_schema2`.`test_distr2`.`test_field4_sql`",
        },
        {
            "link_type": CompositeLinkTypeEnum.SELECT,
            "datasources": [
                datasources[4],
            ],
            "links_fields": [],
            "datasources_info_dict": {
                "test_datasource1": {
                    "source": datasources[4]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {
                        "test_field1": DatasourceFieldMock("test_field1_sql"),
                        "test_field3": DatasourceFieldMock("test_field3_sql"),
                    },
                },
            },
            "fields": [
                {
                    "name": "test_composite_field",
                    "sql_name": "test_composite_field_sql",
                    "datasource_links": [
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        },
                    ],
                }
            ],
            "model": model_model_list[0],
            "result": "SELECT `test_schema1`.`test_distr1`.`test_field1_sql` as `test_composite_field_sql` FROM `test_schema1`.`test_distr1`",
        },
        {
            "link_type": CompositeLinkTypeEnum.INNER_JOIN,
            "datasources": [
                datasources[4],
                datasources[1],
            ],
            "links_fields": [],
            "datasources_info_dict": {
                "test_datasource1": {
                    "source": datasources[4]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {
                        "test_field1": DatasourceFieldMock("test_field1_sql"),
                        "test_field3": DatasourceFieldMock("test_field3_sql"),
                    },
                },
                "test_datasource2": {
                    "source": datasources[1]["source"],
                    "type": CompositeFieldRefObjectEnum.DATASTORAGE,
                    "fields": {
                        "test_field2": DatasourceFieldMock("test_field_sql"),
                        "test_field4": DatasourceFieldMock("test_field4_sql"),
                    },
                },
            },
            "fields": [
                {
                    "name": "test_composite_field",
                    "sql_name": "test_composite_field_sql",
                    "datasource_links": [
                        {
                            "datasource": "test_datasource1",
                            "datasource_field": "test_field1",
                        },
                    ],
                }
            ],
            "model": model_model_list[0],
            "result": "SELECT `test_schema1`.`test_distr1`.`test_field1_sql` as "
            + "`test_composite_field_sql` FROM `test_schema1`.`test_distr1` GLOBAL INNER JOIN `test_schema2`.`test_distr2`"
            + " ON `test_schema1`.`test_distr1`.`test_field1_sql` = `test_schema2`.`test_distr2`.`test_field_sql`"
            + " AND `test_schema1`.`test_distr1`.`test_field3_sql` = `test_schema2`.`test_distr2`.`test_field4_sql`",
        },
    ]


@pytest.fixture
def cases_generate_composite_sql_expression_by_create_parameters(
    cases_generate_composite_join_sql_expression: list[dict],
    cases_for_generate_composite_union_sql_expression: list[dict],
) -> list[dict]:
    postgre_model = model_model_list[0].model_copy(deep=True)
    if not postgre_model.database:
        raise ValueError("Not found database")
    postgre_model.database.type = DatabaseTypeEnum.POSTGRESQL
    return [
        {
            "link_type": CompositeLinkTypeEnum.UNION,
            "datasources_info_dict": cases_for_generate_composite_union_sql_expression[0]["datasources_info_dict"],
            "fields": cases_for_generate_composite_union_sql_expression[0]["fields"],
            "datasources": cases_for_generate_composite_union_sql_expression[0]["datasources"],
            "links_fields": [],
            "model": postgre_model,
            "result": 'SELECT "test_schema1"."test_dbo1"."test_field1_sql" as '
            + '"test_composite_field_sql" FROM "test_schema1"."test_dbo1" UNION ALL SELECT'
            + ' "test_schema2"."test_distr2"."test_field_sql" as "test_composite_field_sql" FROM "test_schema2"."test_distr2"',
        },
        {
            "link_type": CompositeLinkTypeEnum.INNER_JOIN,
            "datasources_info_dict": cases_generate_composite_join_sql_expression[0]["datasources_info_dict"],
            "fields": cases_generate_composite_join_sql_expression[0]["fields"],
            "datasources": cases_generate_composite_join_sql_expression[0]["datasources"],
            "links_fields": cases_generate_composite_join_sql_expression[0]["links_fields"],
            "model": postgre_model,
            "result": 'SELECT "test_schema1"."test_distr1"."test_field1_sql" as "test_composite_field_sql" FROM'
            + ' "test_schema1"."test_distr1" GLOBAL INNER JOIN "test_schema2"."test_distr2" ON '
            + '"test_schema1"."test_distr1"."test_field1_sql" = "test_schema2"."test_distr2"."test_field_sql" AND'
            + ' "test_schema1"."test_distr1"."test_field3_sql" = "test_schema2"."test_distr2"."test_field4_sql"',
        },
    ]
