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

        self.add_vertex(int(timestamp), src_id, Properties(ImmutableProperty("name", src_node)), Type("Character"))
        self.add_vertex(int(timestamp), tar_id, Properties(ImmutableProperty("name", target_node)), Type("Character"))
        self.add_edge(int(timestamp), src_id, tar_id, Type("Character Co-occurence"))


class CCStep1(Step):
    def eval(self, v: Vertex):
        v['cclabel'] = v.id()
        v.message_all_neighbours(v.id())


class CCIterate1(Iterate):
    def __init__(self, iterations: int, execute_messaged_only: bool):
        super().__init__(iterations, execute_messaged_only)

    def eval(self, v: Vertex):
        label = min(v.message_queue())
        if label < v['cclabel']:
            v['cclabel'] = label
            v.message_all_neighbours(label)
        else:
            v.vote_to_halt()


class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            return self.rg.at(32674) \
                .past() \
                .step(CCStep1()) \
                .iterate(CCIterate1(iterations=100, execute_messaged_only=True)) \
                .select(['cclabel']) \
                .write_to_file("/tmp/pyraphtory_output")
        except Exception as e:
            print(str(e))
            traceback.print_exc()
