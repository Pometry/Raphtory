from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row
from pyraphtory.vertex import Vertex


IN_DEGREE = 'inDegree'
OUT_DEGREE = 'outDegree'
DEGREE = 'degree'

class Degree(PyAlgorithm):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step(v: Vertex):
            v[IN_DEGREE] = v.in_degree()
            v[OUT_DEGREE] = v.out_degree()
            v[DEGREE] = v.degree()

        return graph.step(step)

    def tabularise(self, graph: TemporalGraph):
        return graph.select(lambda v: Row(v.name(), v[IN_DEGREE], v[OUT_DEGREE], v[DEGREE]))
