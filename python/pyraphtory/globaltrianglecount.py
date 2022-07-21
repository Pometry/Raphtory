import traceback

# from localtrianglecount import LocalTriangleCount
from pyraphtory.steps import Vertex, NumAdder, StepState, Step
from pyraphtory.builder import *
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph
from pyraphtory.vertex import GlobalState


class LotrGraphBuilder(BaseBuilder):
    def __init__(self):
        super(LotrGraphBuilder, self).__init__()

    def parse_tuple(self, line: str):
        src_node, target_node, timestamp, *_ = line.split(",")

        src_id = self.assign_id(src_node)
        tar_id = self.assign_id(target_node)

        self.add_vertex(int(timestamp), src_id, [ImmutableProperty("name", src_node)], "Character")
        self.add_vertex(int(timestamp), tar_id, [ImmutableProperty("name", target_node)], "Character")
        self.add_edge(int(timestamp), src_id, tar_id, [], "Character Co-occurence")


class GTCStep1(StepState):
    def eval(self, v: Vertex, gs: GlobalState):
        tri = v['triangleCount']
        gs['triangles'].update(tri)


class GTCStep2(StepState):
    def eval(self, v: Vertex, gs: GlobalState):
        print('step2')
        print(gs['triangles'].value())
        gs['triangles'].update(int(gs['triangles'].value() / 3))

class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            local_triangle_count = LocalTriangleCount()
            graph =  self.rg.at(32674).past()
            return local_triangle_count(graph) \
                .set_global_state(NumAdder(name="triangles", retain_state=True)) \
                .step_state(GTCStep1()) \
                .step_state(GTCStep2()) \
                .select_state(["triangles"]) \
                .write_to_file("/tmp/pyraphtory_global_triangle_state")
        except Exception as e:
            print(str(e))
            traceback.print_exc()


## HELLO ITS ME
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



