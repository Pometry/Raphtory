from pyraphtory.api.algorithm import PyAlgorithm
from pyraphtory.api.graph import TemporalGraph
from pyraphtory.api.table import Table
from pyraphtory.api.vertex import Vertex

CC_LABEL = 'cclabel'


class ConnectedComponents(PyAlgorithm):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step(v: Vertex):
            v[CC_LABEL] = v.id()
            v.message_all_neighbours(v.id())

        def iterate(v: Vertex):
            label = min(v.message_queue())
            if label < v[CC_LABEL]:
                v[CC_LABEL] = label
                v.message_all_neighbours(label)
            else:
                v.vote_to_halt()

        return graph.step(step).iterate(iterate, 100, True)

    def tabularise(self, graph: TemporalGraph) -> Table:
        return graph.select("name", CC_LABEL)
