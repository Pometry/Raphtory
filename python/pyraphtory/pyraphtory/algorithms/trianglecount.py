from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row
from pyraphtory.vertex import Vertex
from pyraphtory.scala.implicits.numeric import Long


class LocalTriangleCount(PyAlgorithm):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step1(v: Vertex):
            v["triangleCount"] = 0
            neighbours = {n: False for n in v.neighbours()}
            v.message_all_neighbours(neighbours)

        def step2(v: Vertex):
            neighbours = v.neighbours()
            queue = v.message_queue()
            tri = 0
            for msg in queue:
                tri += len(set(neighbours).intersection(msg.keys()))
            v['triangleCount'] = int(tri / 2)

        return graph.step(step1).step(step2)

    def tabularise(self, graph: TemporalGraph):
        return graph.select(lambda v: Row(v.name(), v['triangleCount']))


class GlobalTriangleCount(LocalTriangleCount):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        return (super().__call__(graph)
                .set_global_state(lambda s: s.new_adder[Long](name="triangles", initial_value=0, retain_state=True))
                .step(lambda v, s: s["triangles"].add(v["triangleCount"])))

    def tabularise(self, graph: TemporalGraph):
        return graph.global_select(lambda s: Row(s["triangles"].value() // 3))
