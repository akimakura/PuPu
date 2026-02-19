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

from src.integrations.modelling_tools_api.codegen import CompositeGet as Composite
from src.models.composite import CompositeFieldRefObjectEnum

logger = EPMPYLogger(__name__)


class GraphService:
    def get_cleared_cyclic_composites(
        self, composites: list[Composite], di_graph: DiGraph
    ) -> tuple[list[Composite], dict[str, list[Any]]]:
        """
        Получить список композитов без циклов.
        Args:
            composites (list[Composite]): Список композитов
            di_graph (DiGraph):  ориентированный граф композитов
        Returns:
            tuple[list[Composite], dict[str, list[Any]]]: ортеж состоящий из списка композитов
            без цикла и словаря с цикличными композитами
        """
        result: list[Composite] = []
        cycles = list(simple_cycles(di_graph))
        cycles_dict: dict[str, list[Any]] = defaultdict(list)
        for cycle in cycles:
            for composite_name in cycle:
                cycles_dict[composite_name].append(cycle)
        for composite in composites:
            if composite.name in cycles_dict:
                continue
            for datasource in composite.datasources:
                if datasource.type == CompositeFieldRefObjectEnum.COMPOSITE and datasource.name in cycles_dict:
                    cycles_dict[composite.name].append(datasource.name)
                    break
            else:
                result.append(composite)
        return result, cycles_dict

    def get_digraph_by_composites(self, composites: list[Composite]) -> DiGraph:
        """
        Построить ориентированный граф на базе списка композитов.

        Args:
            composites (list[Composite]): Список композитов
        Returns:
            DiGraph: ориентированный граф
        """
        di_graph = DiGraph()
        for composite in composites:
            if any([ds.type == CompositeFieldRefObjectEnum.CE_SCENARIO for ds in composite.datasources]):
                logger.debug("Skip composite %S with CE_SCENARIO datasource", composite.name)
                continue
            di_graph.add_node(composite.name)
            for datasource in composite.datasources:
                if datasource.type == CompositeFieldRefObjectEnum.COMPOSITE:
                    di_graph.add_edge(composite.name, datasource.name)
        return di_graph

    def get_topological_order_composites_names_without_cycles(
        self, composites: list[Composite]
    ) -> tuple[list[str], dict[str, list[str]]]:
        """
        Получить обратный топологический порядок всех композитов без цикличных.

        Args:
            composites (list[Composite]): Список композитов
        Returns:
            tuple[list[str], dict[str, list[str]]]: Кортеж состоящий из списка композитов
            в обратном топологическом порядке и словаря с цикличными композитами
        """
        ignored: dict[str, list[str]] = {}
        di_graph = self.get_digraph_by_composites(composites)
        try:
            result = list(topological_sort(di_graph))
        except NetworkXUnfeasible:
            composites, ignored = self.get_cleared_cyclic_composites(composites, di_graph)
            result = self.get_topological_order_composites_names(composites)
        return result, ignored

    def get_topological_order_composites_names(self, composites: list[Composite]) -> list[str]:
        """
        Получить обратный топологический порядок всех композитов.

        Args:
            composites (list[Composite]): Список композитов
        Returns:
            list[str]: список композитов
            в обратном топологическом порядке
        """
        di_graph = self.get_digraph_by_composites(composites)
        try:
            result = list(list(topological_sort(di_graph)))
        except NetworkXUnfeasible as exc:
            cycles = simple_cycles(di_graph)
            reason = f"Error topological sort. Cycles: {cycles}. "
            logger.exception("Error topological sort")
            raise NetworkXUnfeasible(reason + str(exc))
        return result

    def get_topological_order_composites(self, composites: list[Composite]) -> list[Composite]:
        composite_dict = {composite.name: composite for composite in composites}
        names = self.get_topological_order_composites_names(composites)
        return [composite_dict[name] for name in names if name in composite_dict]

    def get_topological_order_composites_without_cycles(
        self, composites: list[Composite]
    ) -> tuple[list[Composite], dict[str, list[str]]]:
        composite_dict = {composite.name: composite for composite in composites}
        names, ignored = self.get_topological_order_composites_names_without_cycles(composites)
        return [composite_dict[name] for name in names if name in composite_dict], ignored
