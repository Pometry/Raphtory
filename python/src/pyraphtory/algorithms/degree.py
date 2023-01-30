from pyraphtory.api.algorithm import PyAlgorithm
from pyraphtory.api.graph import TemporalGraph, Row
from pyraphtory.api.vertex import Vertex


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
        return graph.step(lambda v: v.set_state("name", v.name()))\
                    .step(lambda v: v.set_state(IN_DEGREE, v[IN_DEGREE]))\
                    .step(lambda v: v.set_state(OUT_DEGREE,v[OUT_DEGREE]))\
                    .step(lambda v: v.set_state(DEGREE, v[DEGREE]))\
                    .select("name", IN_DEGREE, OUT_DEGREE, DEGREE)
