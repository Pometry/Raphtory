import traceback

from pyraphtory.steps import Vertex, Iterate, Step
from pyraphtory.builder import *
from pyraphtory.context import BaseContext


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
    def eval(self, v: Vertex):
        label = min(v.message_queue())
        if label < v['cclabel']:
            v['cclabel'] = label
            v.message_all_neighbours(label)
        else:
            v.vote_to_halt()


class RaphtoryContext(BaseContext):
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


def total_degree(v, s):
    deg_sum = s("deg_sum")
    deg_sum += v.degree()


if __name__ == "__main__":
    from pathlib import Path
    from pyraphtory.context import PyRaphtory
    from pyraphtory.scala.numeric import Int
    import subprocess

    subprocess.run(["curl", "-o", "/tmp/lotr.csv", "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"])
    pr = PyRaphtory(spout_input=Path('/tmp/lotr.csv'), builder_script=Path(__file__), builder_class='LotrGraphBuilder',
                    mode='batch', logging=False).open()
    rg = pr.graph()
    # local_sink = pr.local_sink()
    #
    # cols = ["inDegree", "outDegree", "degree","triangleCount","prlabel","cclabel","twoHopPaths"]
    #
    # tracker = rg.at(32674) \
    #     .past() \
    #     .transform(pr.page_rank()) \
    #     .transform(pr.connected_components()) \
    #     .transform(pr.degree()) \
    #     .transform(pr.two_hops_path(set([]))) \
    #     .transform(pr.local_triangle_count()) \
    #     .select(cols) \
    #     .write_to_file("/tmp/pyraphtory_output") \
    #     .wait_for_job()

    # rg.at(32674).past().execute(pr.algorithms.generic.ConnectedComponents).write_to_file("/tmp/pyraphtory_output").wait_for_job()
    df = (rg.set_global_state(lambda s: s.new_adder[Int]("deg_sum"))
            .step(total_degree)
            .global_select(lambda s: [s("deg_sum").value()])
            .write_to_dataframe(["deg_sum"]))
