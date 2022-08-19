from pyraphtory.graph import Row
from pyraphtory.interop import to_python, find_class
from pyraphtory.builder import *
from pyraphtory.spouts import FileSpout

if __name__ == "__main__":
    from pyraphtory.context import PyRaphtory
    from pyraphtory.scala.numeric import Int
    import subprocess

    subprocess.run(["curl", "-o", "/tmp/lotr.csv", "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"])
    pr = PyRaphtory(logging=True).open()

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

    lotr_builder = GraphBuilder(parse)
    lotr_spout = FileSpout("/tmp/lotr.csv")
    graph = pr.new_graph().ingest(Source(lotr_spout, lotr_builder)).at(32674).past()

    df = (graph
          .select(lambda vertex: Row(vertex.name(), vertex.degree()))
          .write_to_dataframe(["name", "degree"]))
    print(df)

    graph.select(lambda vertex: Row(vertex.name(), vertex.degree())).write_to_file("/tmp/test").wait_for_job()

    #
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
