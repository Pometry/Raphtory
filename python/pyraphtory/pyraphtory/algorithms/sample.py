from pyraphtory.graph import Row
from pyraphtory.vertex import Vertex
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


def CCStep1(v: Vertex):
    v['cclabel'] = v.id()
    v.message_all_neighbours(v.id())


def CCIterate1(v: Vertex):
    label = min(v.message_queue())
    if label < v['cclabel']:
        v['cclabel'] = label
        v.message_all_neighbours(label)
    else:
        v.vote_to_halt()


class RaphtoryContext(BaseContext):
    def eval(self):
        logger.trace("evaluating")
        tracker = self.rg.at(32674) \
            .past() \
            .step(CCStep1) \
            .select(['cclabel']) \
            .write_to_file("/tmp/pyraphtory_output")
        print(tracker)
        return tracker


if __name__ == "__main__":
    from pathlib import Path
    from pyraphtory.context import PyRaphtory
    from pyraphtory.scala.numeric import Int
    import subprocess

    subprocess.run(["curl", "-o", "/tmp/lotr.csv", "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"])
    pr = PyRaphtory(spout_input=Path('/tmp/lotr.csv'), builder_script=Path(__file__), builder_class='LotrGraphBuilder',
                    mode='batch', logging=False).open()
    # pr = PyRaphtory(spout_input=Path('/tmp/nodata.csv'), builder_script=Path(__file__), builder_class='LotrGraphBuilder',
    #                 mode='batch', logging=False).open()
    rg = pr.graph()

    # t = Type("test")
    df = (rg.set_global_state(lambda s: s.new_adder[Int]("deg_sum"))
            .step(lambda v, s: s["deg_sum"].add(v.degree()))
            .global_select(lambda s: Row(s("deg_sum").value()))
            .write_to_dataframe(["deg_sum"]))

    # df = (rg.select(lambda vertex: Row(vertex.name(), vertex.degree()))
    #       .write_to_dataframe(["name", "degree"]))
