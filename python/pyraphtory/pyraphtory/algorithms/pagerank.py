from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex


class PageRank(PyAlgorithm):
    def __init__(self, damping_factor: float = 0.85, max_steps: int = 100):
        self.damping_factor = damping_factor
        self.max_steps = max_steps

    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def init(v: Vertex):
            initLabel = 1.0
            v["prlabel"] = initLabel
            out_degree = v.out_degree()
            if out_degree > 0:
                msg = initLabel / out_degree
                v.message_out_neighbours(msg)

        def iterate(v: Vertex):
            current_label = v["prlabel"]
            queue = v.message_queue()
            summed_queue = sum(queue)
            new_label = (1 - self.damping_factor) + self.damping_factor * summed_queue
            v["prlabel"] = new_label

            out_degree = v.out_degree()

            if out_degree > 0:
                v.message_out_neighbours(new_label / out_degree)

            if abs(new_label - current_label) < 0.00001:
                v.vote_to_halt()

        return graph.step(init).iterate(iterate, self.max_steps, False)

    def tabularise(self, graph: TemporalGraph) -> Table:
        return graph.select(lambda v: Row(v.name(), v["prlabel"]))
