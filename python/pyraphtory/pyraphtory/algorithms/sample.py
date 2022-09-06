from pyraphtory.graph import Row
from pyraphtory.builder import *
from pyraphtory.spouts import FileSpout
from time import perf_counter
from pyraphtory.algorithms.pagerank import PageRank
from pyraphtory.algorithms.connectedcomponents import ConnectedComponents
from pyraphtory.algorithms.trianglecount import LocalTriangleCount, GlobalTriangleCount
from pyraphtory.algorithms.twohoppaths import TwoHopPaths
from pyraphtory.algorithms.degree import Degree

if __name__ == "__main__":
    from pyraphtory.context import PyRaphtory
    import subprocess

    subprocess.run(["curl", "-o", "/tmp/lotr.csv", "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"])
    pr = PyRaphtory(logging=True).open()

    def parse(graph, tuple: str):
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
    graph = pr.new_graph().load(Source(lotr_spout, lotr_builder))
    result = (graph
              .select(lambda vertex: Row(vertex.name(), vertex.degree()))
              .collect())
    result.wait_for_job()

    # df = (graph
    #       .select(lambda vertex: Row(vertex.name(), vertex.degree()))
    #       .write_to_dataframe(["name", "degree"]))
    # print(df)
    #
    # df = (graph.execute(PageRank()).write_to_dataframe(["name", "pagerank"]))
    # print(df)
    #
    # df = (graph.execute(ConnectedComponents()).write_to_dataframe(["name", "component"]))
    # print(df)
    #
    # df = (graph.execute(TwoHopPaths()).write_to_dataframe(["start", "middle", "end"]))
    # print(df)
    #
    # df = (graph.execute(LocalTriangleCount()).write_to_dataframe(["name", "triangles"]))
    # print(df)
    #
    # df = (graph.execute(GlobalTriangleCount()).write_to_dataframe(["triangles"]))
    # print(df)
    #
    # df = (graph.execute(Degree()).write_to_dataframe(["name", "in-degree", "out-degree", "degree"]))
    # print(df)
    #
    # graph2 = pr.new_graph()
    # # can just call add_vertex, add_edge on graph directly without spout/builder
    # start = perf_counter()
    # graph2.block_ingestion()
    # with open("/tmp/lotr.csv") as f:
    #     for line in f:
    #         parse(graph2, line)
    # graph2.unblock_ingestion()
    # print(f"time taken to ingest: {perf_counter()-start}s")
    #
    # df = (graph2
    #       .select(lambda vertex: Row(vertex.name(), vertex.degree()))
    #       .write_to_dataframe(["name", "degree"]))
    # print(df)
    #
    # df2 = (graph2
    #        .select(lambda v: Row(v.name(), v.latest_activity().time()))
    #        .write_to_dataframe(["name", "latest_time"]))
    # print(df2)
    #
    # def accum_step(v, s):
    #     ac = s["max_time"]
    #     latest = v.latest_activity().time()
    #     ac += latest
    #
    # df2 = (graph
    #        .set_global_state(lambda s: s.new_accumulator("max_time", 0, op=lambda a, b: max(a, b)))
    #        .step(accum_step)
    #        .global_select(lambda s: Row(s["max_time"].value()))
    #        .write_to_dataframe(["max_time"]))
    # print(df2)
    #
    # graph.select(lambda vertex: Row(vertex.name(), vertex.degree())).write_to_file("/tmp/test").wait_for_job()

