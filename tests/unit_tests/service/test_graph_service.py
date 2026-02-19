import pytest
from networkx.exception import NetworkXUnfeasible

from src.service.graph import GraphService
from tests.unit_tests.fixtures.dimension import test_graph_dimension

result_dimension = (
    ["dim_graph3"],
    {
        "dim_graph1": [
            [
                "dim_graph1",
            ],
        ],
        "dim_graph2": [
            "dim_graph1",
        ],
    },
)


class TestGraphService:

    def test_get_reversed_topological_order_dimensions_without_cycles(self) -> None:
        assert result_dimension == GraphService.get_reversed_topological_order_dimensions_without_cycles(
            test_graph_dimension
        )

    def test_get_reversed_topological_order_dimensions(self) -> None:
        with pytest.raises(NetworkXUnfeasible):
            _ = GraphService.get_reversed_topological_order_dimensions(test_graph_dimension)
