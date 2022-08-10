from pyraphtory.graph import TemporalGraph, Row, Table


class PyAlgorithm(object):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        return graph

    def tabularise(self, graph: TemporalGraph) -> Table:
        graph.global_select(lambda s: Row())
