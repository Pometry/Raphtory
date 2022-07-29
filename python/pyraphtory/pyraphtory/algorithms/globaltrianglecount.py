import traceback

from typing import Any

from pyraphtory.steps import Vertex, NumAdder, StepState, Step, GlobalSelect
from pyraphtory.builder import *
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph
from pyraphtory.vertex import GraphState


class GTCStep1(StepState):
    def eval(self, v: Vertex, gs: GraphState):
        tri = v['triangleCount']
        gs['triangles'].update(tri)

class GTCGS1(GlobalSelect):
    def eval(self, gs: GraphState) -> List[Any]:
        division = int(gs['triangles'].value() / 3)
        return [division]

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
        v['triangleCount'] = int(tri / 2)


class LocalTriangleCount(object):
    def __call__(self, graph: TemporalGraph, *args, **kwargs) -> TemporalGraph:
        return graph.step(LTCStep1()) \
            .set_global_state(NumAdder(name="triangles", retain_state=True)) \
            .step(LTCStep2())

class GlobalTriangleCount(object):
    def __call__(self, graph: TemporalGraph, *args, **kwargs) -> TemporalGraph:
        return graph.step(LTCStep1()) \
            .set_global_state(NumAdder(name="triangles", retain_state=True)) \
            .step(LTCStep2())


class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            local_triangle_count = LocalTriangleCount()
            graph = self.rg.at(32674).past()
            return local_triangle_count(graph) \
                .set_global_state(NumAdder(name="triangles", retain_state=True)) \
                .step_state(GTCStep1()) \
                .global_select(GTCGS1()) \
                .write_to_file("/tmp/pyraphtory_global_triangle_state")
        except Exception as e:
            print(str(e))
            traceback.print_exc()
