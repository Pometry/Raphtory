import traceback

from pyraphtory.steps import Vertex, Iterate, Step
from pyraphtory.builder import *
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph


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


IN_DEGREE = 'inDegree'
OUT_DEGREE = 'outDegree'
DEGREE = 'degree'

class DegreeStep(Step):
    def eval(self, v: Vertex):
        v[IN_DEGREE] = v.in_degree()
        v[OUT_DEGREE] = v.out_degree()
        v[DEGREE] = v.degree()

class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            return self.rg.at(32674) \
                .past() \
                .step(DegreeStep()) \
                .select([IN_DEGREE, OUT_DEGREE, DEGREE]) \
                .write_to_file("/tmp/pyraphtory_degree")
        except Exception as e:
            print(str(e))
            traceback.print_exc()
