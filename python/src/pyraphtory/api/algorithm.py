"""Base classes for writing algorithms"""
from __future__ import annotations

from pyraphtory.interop import GenericScalaProxy


class ScalaAlgorithm(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.algorithm.BaseAlgorithm"


class PyAlgorithm(object):
    """
    Base class for algorithms implemented in python
    """
    def __call__(self, graph: graph.TemporalGraph) -> graph.TemporalGraph:
        """
        Main algorithm step (default implementation leaves graph unchanged)

        :param graph: input graph
        :return: transformed graph
        """
        return graph

    def tabularise(self, graph: graph.TemporalGraph) -> table.Table:
        """
        Defines the default output of the algorithm

        (default implementation includes all vertex state in the table)

        :param graph: output graph after algorithm step was applied
        :return: Table with output from this algorithm
        """
        return graph.select()

    def name(self):
        return self.__class__.__name__
