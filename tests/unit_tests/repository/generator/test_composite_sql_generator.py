from contextlib import nullcontext as does_not_raise
from typing import Any

import pytest

from src.repository.generators.composite_sql_generator import CompositeSqlGenerator


class TestCompositeSqlGenerator:

    @pytest.mark.parametrize(
        ("case_num"),
        [
            (0),
            (1),
        ],
    )
    def test_generate_field_name(
        self, monkeypatch: pytest.MonkeyPatch, cases_for_generate_field_name: list[dict], case_num: int
    ) -> None:
        case_for_generate_field_name = cases_for_generate_field_name[case_num]

        def mock_get_datasource_schema_name_and_datasource_name(datasource: Any, model: Any) -> Any:
            return case_for_generate_field_name["schema_name"], case_for_generate_field_name["datasource_name"]

        monkeypatch.setattr(
            CompositeSqlGenerator,
            "_get_datasource_schema_name_and_datasource_name",
            mock_get_datasource_schema_name_and_datasource_name,
        )
        assert (
            CompositeSqlGenerator._generate_field_name(
                case_for_generate_field_name["datasources_info_dict"],
                case_for_generate_field_name["field"],
                case_for_generate_field_name["model"],
                case_for_generate_field_name["datasource_name"],
            )
            == case_for_generate_field_name["result"]
        )

    @pytest.mark.parametrize(
        ("case_num", "expected_exception"),
        [
            (0, does_not_raise()),
            (1, does_not_raise()),
            (2, does_not_raise()),
            (3, does_not_raise()),
            (4, pytest.raises(ValueError)),
        ],
    )
    def test_get_datasource_schema_name_and_datasource_name(
        self,
        cases_get_datasource_schema_name_and_datasource_name: list[dict],
        case_num: int,
        expected_exception: pytest.RaisesExc,
    ) -> None:
        case_get_datasource_schema_name_and_datasource_name = cases_get_datasource_schema_name_and_datasource_name[
            case_num
        ]
        with expected_exception:
            assert (
                CompositeSqlGenerator._get_datasource_schema_name_and_datasource_name(
                    case_get_datasource_schema_name_and_datasource_name["datasource"],
                    case_get_datasource_schema_name_and_datasource_name["model"],
                )
                == case_get_datasource_schema_name_and_datasource_name["result"]
            )

    @pytest.mark.parametrize(
        ("case_num", "expected_exception"),
        [
            (0, does_not_raise()),
            (1, does_not_raise()),
            (2, does_not_raise()),
        ],
    )
    def test_generate_composite_union_sql_expression(
        self,
        cases_for_generate_composite_union_sql_expression: list[dict],
        case_num: int,
        expected_exception: pytest.RaisesExc,
    ) -> None:
        case_for_generate_composite_union_sql_expression = cases_for_generate_composite_union_sql_expression[case_num]
        with expected_exception:
            assert (
                CompositeSqlGenerator._generate_composite_union_sql_expression(
                    case_for_generate_composite_union_sql_expression["datasources_info_dict"],
                    case_for_generate_composite_union_sql_expression["fields"],
                    case_for_generate_composite_union_sql_expression["datasources"],
                    case_for_generate_composite_union_sql_expression["model"],
                )
                == case_for_generate_composite_union_sql_expression["result"]
            )

    @pytest.mark.parametrize(
        ("case_num", "expected_exception"),
        [
            (0, does_not_raise()),
            (1, does_not_raise()),
            (2, pytest.raises(ValueError)),
        ],
    )
    def test_generate_composite_join_sql_expression(
        self,
        cases_generate_composite_join_sql_expression: list[dict],
        case_num: int,
        expected_exception: pytest.RaisesExc,
    ) -> None:
        case_generate_composite_join_sql_expression = cases_generate_composite_join_sql_expression[case_num]
        with expected_exception:
            assert (
                CompositeSqlGenerator._generate_composite_join_sql_expression(
                    case_generate_composite_join_sql_expression["link_type"],
                    case_generate_composite_join_sql_expression["datasources_info_dict"],
                    case_generate_composite_join_sql_expression["fields"],
                    case_generate_composite_join_sql_expression["datasources"],
                    case_generate_composite_join_sql_expression["links_fields"],
                    case_generate_composite_join_sql_expression["model"],
                )
                == case_generate_composite_join_sql_expression["result"]
            )

    @pytest.mark.parametrize(
        ("case_num"),
        [
            (0),
            (1),
        ],
    )
    def test_generate_composite_sql_expression_by_create_parameters(
        self,
        cases_generate_composite_sql_expression_by_create_parameters: list[dict],
        case_num: int,
    ) -> None:
        case_generate_composite_join_sql_expression = cases_generate_composite_sql_expression_by_create_parameters[
            case_num
        ]
        assert (
            CompositeSqlGenerator.generate_composite_sql_expression_by_create_parameters(
                case_generate_composite_join_sql_expression["link_type"],
                case_generate_composite_join_sql_expression["datasources_info_dict"],
                case_generate_composite_join_sql_expression["fields"],
                case_generate_composite_join_sql_expression["datasources"],
                case_generate_composite_join_sql_expression["links_fields"],
                case_generate_composite_join_sql_expression["model"],
            )
            == case_generate_composite_join_sql_expression["result"]
        )
