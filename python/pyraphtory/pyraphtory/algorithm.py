from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.interop import ScalaProxyBase, find_class, to_python


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


class BuiltinAlgorithm(ScalaProxyBase):
    """Proxy object for looking up built-in algorithms based on path

    (This actually could be used for looking up other classes as well if needed)
    """
    @property
    def _jvm_object(self):
        return find_class(self._path)

    def __init__(self, path: str):
        self._path = path

    def __call__(self, *args, **kwargs):
        return to_python(self._jvm_object).apply(*args, **kwargs)

    def __getattr__(self, item):
        return BuiltinAlgorithm(".".join((self._path, item)))