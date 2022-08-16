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
        tracker = self.rg \
            .step(CCStep1) \
            .iterate(CCIterate1, 100, True) \
            .select(lambda v: Row(v.name(), v['cclabel'])) \
            .write_to_file("/tmp/pyraphtory_output")
        logger.trace("query submitted")
        return tracker


if __name__ == "__main__":
    from pathlib import Path
    from pyraphtory.context import PyRaphtory
    from pyraphtory.scala.numeric import Int
    import subprocess

    subprocess.run(["curl", "-o", "/tmp/lotr.csv", "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"])
    pr = PyRaphtory(logging=True).open()

    from pyraphtory.interop import to_python, find_class, logger
    r = to_python(find_class("com.raphtory.Raphtory"))
    graph = r.new_graph()
    builder = to_python(find_class("com.raphtory.api.input.GraphBuilder"))

    def parse(graph, tuple: str):
        print(f"parse called with {graph=} and {tuple=}")
        parts = [v.strip() for v in tuple.split(",")]
        source_node = parts[0]
        src_id = graph.assign_id(source_node)
        target_node = parts[1]
        tar_id = graph.assign_id(target_node)
        time_stamp = int(parts[2])

        graph.add_vertex(time_stamp, src_id, Properties(ImmutableProperty("name", source_node)), Type("Character"))
        graph.add_vertex(time_stamp, tar_id, Properties(ImmutableProperty("name", target_node)), Type("Character"))
        graph.add_edge(time_stamp, src_id, tar_id, Type("Character_Co-occurence"))

    lotr_builder = builder(parse)
    spout = to_python(find_class("com.raphtory.spouts.FileSpout"))
    lotr_spout = spout("/tmp/lotr.csv")
    source = to_python(find_class("com.raphtory.api.input.Source"))
    scala_list = to_python(find_class("scala.collection.immutable.List"))
    input_args = getattr(scala_list, "from")([source(lotr_spout, lotr_builder)])
    graph.ingest(input_args)

    # pr = PyRaphtory(spout_input=Path('/tmp/nodata.csv'), builder_script=Path(__file__), builder_class='LotrGraphBuilder',
    #                 mode='batch', logging=False).open()
    # rg = pr.graph()
    #
    # # t = Type("test")
    # df = (rg.set_global_state(lambda s: s.new_adder[Int]("deg_sum"))
    #         .step(lambda v, s: s["deg_sum"].add(v.degree()))
    #         .global_select(lambda s: Row(s("deg_sum").value()))
    #         .write_to_dataframe(["deg_sum"]))

    # print(df)
    # # df = (rg.select(lambda vertex: Row(vertex.name(), vertex.degree()))
    # #       .write_to_dataframe(["name", "degree"]))
