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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.dimension import Dimension, DimensionAttribute

logger = EPMPYLogger(__name__)


class GraphRepository:

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_dependency_dimension_ids(self, root_dimension_id: int) -> list[int]:
        """
        Получить список зависимых справочников.
        Args:
            root_dimension_id (int): Идентификатор корневого справочника
        Returns:
            list[int]: Список идентификаторов зависимых справочников
        """
        visited = {root_dimension_id}
        queue = [root_dimension_id]

        while queue:
            current_id = queue.pop(0)

            # dimension_id
            stmt = select(Dimension.dimension_id).where(Dimension.id == current_id)
            dim_id = (await self.session.execute(stmt)).scalar()
            if dim_id and dim_id not in visited:
                queue.append(dim_id)
                visited.add(dim_id)

            # attributes
            stmt_attrs = select(DimensionAttribute.dimension_attribute_id).where(
                DimensionAttribute.dimension_id == current_id, DimensionAttribute.dimension_attribute_id.isnot(None)
            )
            for attr_id in (await self.session.execute(stmt_attrs)).scalars():
                if attr_id and attr_id not in visited:
                    queue.append(attr_id)
                    visited.add(attr_id)

        return list(visited)

    def get_cleared_cyclic_dimensions(
        cls, dimensions: list[Dimension], di_graph: DiGraph
    ) -> tuple[list[Dimension], dict[str, list[Any]]]:
        """
        Получить список справочников без циклов.
        Args:
            dimensions (list[Dimension]): Список справочников
            di_graph (DiGraph):  ориентированный граф справочников
        Returns:
            tuple[list[Dimension], dict[str, list[Any]]]: кортеж состоящий из списка справочников
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
            if dimension.dimension is not None and dimension.dimension.name in cycles_dict:
                cycles_dict[dimension.name].append(dimension.dimension.name)
                continue
            for attribute in dimension.attributes:
                if attribute.dimension_attribute is not None and attribute.dimension_attribute.name in cycles_dict:
                    cycles_dict[dimension.name].append(attribute.dimension_attribute.name)
                    break
            else:
                result.append(dimension)
        return result, cycles_dict

    def get_digraph_by_dimensions(self, dimensions: list[Dimension], pv_flag: bool = False) -> DiGraph:
        """
        Построить ориентированный граф на базе списка справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            DiGraph: ориентированный граф
        """
        di_graph = DiGraph()
        for dimension in dimensions:
            if pv_flag and dimension.pv_dictionary_id is not None:
                logger.debug("Dimension %s already created or virtual. Skip (Graph)", dimension.name)
                continue
            di_graph.add_node(dimension.name)
            if dimension.dimension:
                di_graph.add_edge(dimension.name, dimension.dimension.name)
            else:
                for attribute in dimension.attributes:
                    if attribute.dimension_attribute is not None:
                        di_graph.add_edge(dimension.name, attribute.dimension_attribute.name)
        return di_graph

    def get_reversed_topological_order_dimensions_without_cycles(
        self, dimensions: list[Dimension], pv_flag: bool = False
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
        di_graph = self.get_digraph_by_dimensions(dimensions, pv_flag)
        try:
            result = list(reversed(list(topological_sort(di_graph))))
        except NetworkXUnfeasible:
            dimensions, ignored = self.get_cleared_cyclic_dimensions(dimensions, di_graph)
            result = self.get_reversed_topological_order_dimensions(dimensions)
        return result, ignored

    def get_reversed_topological_order_dimensions(self, dimensions: list[Dimension]) -> list[str]:
        """
        Получить обратный топологический порядок всех справочников.

        Args:
            dimensions (list[Dimension]): Список справочников
        Returns:
            list[str]: список справочников
            в обратном топологическом порядке
        """
        di_graph = self.get_digraph_by_dimensions(dimensions)
        try:
            result = list(reversed(list(topological_sort(di_graph))))
        except NetworkXUnfeasible as exc:
            cycles = simple_cycles(di_graph)
            reason = f"Error topological sort. Cycles: {cycles}. "
            logger.exception("Error topological sort")
            raise NetworkXUnfeasible(reason + str(exc))
        return result
