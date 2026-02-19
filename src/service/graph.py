"""
Сервис для работы с графами.
Тут реализована вся логика работы сервиса.
Работа с БД инкапсулирована через DataRepository.
"""

from collections import defaultdict
from typing import Any

from networkx import DiGraph, simple_cycles, topological_sort
from networkx.exception import NetworkXUnfeasible
from py_common_lib.logger import EPMPYLogger

from src.models.composite import Composite, CompositeFieldRefObjectEnum
from src.models.dimension import Dimension
from src.models.field import BaseFieldTypeEnum

logger = EPMPYLogger(__name__)


class GraphService:

    @classmethod
    def get_cleared_cyclic_dimensions(
        cls, dimensions: list[Dimension], di_graph: DiGraph
    ) -> tuple[list[Dimension], dict[str, list[Any]]]:
        """
        Получить список справочников без циклов.
        Args:
            dimensions (list[Dimension]): Список справочников
            di_graph (DiGraph):  ориентированный граф справочников
        Returns:
            tuple[list[Dimension], dict[str, list[Any]]]: ортеж состоящий из списка справочников
            без цикла и словаря с цикличными справочниками
        """
        result: list[Dimension] = []
        cycles = list(simple_cycles(di_graph))
        cycles_dict: dict[str, list[Any]] = defaultdict(list)
        for cycle in cycles:
            for dimension_name in cycle:
                cycles_dict[dimension_name].append(cycle)
        for dimension in dimensions:
            if dimension.name in cycles_dict:
                continue
            for attribute in dimension.attributes:
                if (
                    attribute.ref_type.ref_object_type == BaseFieldTypeEnum.DIMENSION
                    and attribute.ref_type.ref_object in cycles_dict
                ):
                    cycles_dict[dimension.name].append(attribute.ref_type.ref_object)
                    break
            else:
                result.append(dimension)
        return result, cycles_dict

    @classmethod
    def get_digraph_by_dimensions_attrs(cls, dimensions: list[Dimension]) -> DiGraph:
        """
        Построить ориентированный граф на базе списка справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            DiGraph: ориентированный граф
        """
        di_graph = DiGraph()
        for dimension in dimensions:
            di_graph.add_node(dimension.name)
            for attribute in dimension.attributes:
                if attribute.ref_type.ref_object_type == BaseFieldTypeEnum.DIMENSION:
                    di_graph.add_edge(dimension.name, attribute.ref_type.ref_object)
        return di_graph

    @classmethod
    def get_reversed_topological_order_dimensions_without_cycles(
        cls, dimensions: list[Dimension]
    ) -> tuple[list[str], dict[str, list[str]]]:
        """
        Получить обратный топологический порядок всех справочников без цикличных.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            tuple[list[str], dict[str, list[str]]]: Кортеж состоящий из списка справочников
            в обратном топологическом порядке и словаря с цикличными справочниками
        """
        ignored: dict[str, list[str]] = {}
        di_graph = cls.get_digraph_by_dimensions_attrs(dimensions)
        try:
            result = list(reversed(list(topological_sort(di_graph))))
        except NetworkXUnfeasible:
            dimensions, ignored = cls.get_cleared_cyclic_dimensions(dimensions, di_graph)
            result = cls.get_reversed_topological_order_dimensions(dimensions)
        return result, ignored

    @classmethod
    def get_reversed_topological_order_dimensions(cls, dimensions: list[Dimension]) -> list[str]:
        """
        Получить обратный топологический порядок всех справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            list[str]: список справочников
            в обратном топологическом порядке
        """
        di_graph = cls.get_digraph_by_dimensions_attrs(dimensions)
        try:
            result = list(reversed(list(topological_sort(di_graph))))
        except NetworkXUnfeasible as exc:
            cycles = simple_cycles(di_graph)
            reason = f"Error topological sort. Cycles: {cycles}. "
            logger.exception("Error topological sort")
            raise NetworkXUnfeasible(reason + str(exc))
        return result

    @classmethod
    def get_digraph_by_composite(cls, composites: list[Composite]) -> DiGraph:
        """
        Построить ориентированный граф на базе списка справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            DiGraph: ориентированный граф
        """
        di_graph = DiGraph()
        for composite in composites:
            di_graph.add_node(composite.name)
            for datasource in composite.datasources:
                if datasource.type == CompositeFieldRefObjectEnum.COMPOSITE:
                    di_graph.add_edge(composite.name, datasource.name)
        return di_graph

    @classmethod
    def get_digraph_by_dimensions_ref(cls, dimensions: list[Dimension]) -> DiGraph:
        """
        Построить ориентированный граф на базе списка справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            DiGraph: ориентированный граф
        """
        di_graph = DiGraph()
        for dimension in dimensions:
            di_graph.add_node(dimension.name)
            if dimension.dimension_name:
                di_graph.add_edge(dimension.name, dimension.dimension_name)
        return di_graph

    @classmethod
    def get_topological_order_dimensions_by_ref_dimension(cls, dimensions: list[Dimension]) -> list[Dimension]:
        """
        Получить обратный топологический порядок всех справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            list[str]: список справочников
            в обратном топологическом порядке
        """
        dimensions_map = {}
        di_graph = cls.get_digraph_by_dimensions_ref(dimensions)
        try:
            result = list(reversed(list(topological_sort(di_graph))))
        except NetworkXUnfeasible as exc:
            cycles = simple_cycles(di_graph)
            reason = f"Error topological sort. Cycles: {cycles}. "
            logger.exception("Error topological sort")
            raise NetworkXUnfeasible(reason + str(exc))
        for dimension in dimensions:
            dimensions_map[dimension.name] = dimension
        new_dimensions = []
        for dim_res in result:
            new_dimensions.append(dimensions_map[dim_res])
        return new_dimensions

    @classmethod
    def get_topological_order_composites(cls, composites: list[Composite]) -> list[Composite]:
        """
        Получить обратный топологический порядок всех справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            list[str]: список справочников
            в обратном топологическом порядке
        """
        composite_map = {}
        di_graph = cls.get_digraph_by_composite(composites)
        try:
            result = list(reversed(list(topological_sort(di_graph))))
        except NetworkXUnfeasible as exc:
            cycles = simple_cycles(di_graph)
            reason = f"Error topological sort. Cycles: {cycles}. "
            logger.exception("Error topological sort")
            raise NetworkXUnfeasible(reason + str(exc))
        for composite in composites:
            composite_map[composite.name] = composite
        new_composites = []
        for com_res in result:
            new_composites.append(composite_map[com_res])
        return new_composites
