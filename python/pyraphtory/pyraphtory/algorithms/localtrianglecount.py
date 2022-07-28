import traceback

from pyraphtory.steps import Vertex, Iterate, Step, NumAdder
from pyraphtory.builder import *
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph


class LTCStep1(Step):
    def eval(self, v: Vertex):
        v["triangleCount"] = 0
        neighbours = {n: False for n in v.neighbours()}
        v.message_all_neighbours(neighbours)


class LTCStep2(Step):
    def eval(self, v: Vertex):
        neighbours = v.neighbours()
        queue = v.message_queue()
        tri = 0
        for msg in queue:
            tri += len(set(neighbours).intersection(msg.keys()))
        v['triangleCount'] = tri / 2


class LocalTriangleCount(object):
    def __call__(self, graph: TemporalGraph, *args, **kwargs) -> TemporalGraph:
        return graph.step(LTCStep1()) \
            .set_global_state(NumAdder(name="triangles", retain_state=True)) \
            .step(LTCStep2())


class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            ltc = LocalTriangleCount()
            graph = self.rg.at(32674).past()
            return ltc(graph) \
                .select(["triangleCount"]) \
                .write_to_file("/tmp/pyraphtory_local_triangle_state")
        except Exception as e:
            print(str(e))
            traceback.print_exc()
