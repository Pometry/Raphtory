from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.interop import GenericScalaProxy, ScalaClassProxy


class ScalaAlgorithm(GenericScalaProxy):
    _classname = "com.raphtory.api.algorithm.BaseAlgorithm"


class Alignment(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.graphview.Alignment"


class PyAlgorithm(object):
    """
    Base class for algorithms implemented in python
    """
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        """
        Main algorithm step (default implementation leaves graph unchanged)

        :param graph: input graph
        :return: transformed graph
        """
        return graph

    def tabularise(self, graph: TemporalGraph) -> Table:
        """
        Defines the default output of the algorithm

        (default implementation creates an empty table)

        :param graph: output graph after algorithm step was applied
        :return: Table with output from this algorithm
        """
        return graph.global_select(lambda s: Row())
