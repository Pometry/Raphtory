"""
sample python script.
"""

from pyraphtory.graph import Row
from pyraphtory.input import *
from pyraphtory.sources import Source
from pyraphtory.scala.implicits.numeric import Long
from pyraphtory.spouts import FileSpout
from time import perf_counter
from pyraphtory.algorithms.pagerank import PageRank
from pyraphtory.algorithms.connectedcomponents import ConnectedComponents
from pyraphtory.algorithms.trianglecount import LocalTriangleCount, GlobalTriangleCount
# from pyraphtory.algorithms.twohoppaths import TwoHopPaths
from pyraphtory.algorithms.degree import Degree

if __name__ == "__main__":
    from pyraphtory.context import PyRaphtory
    import subprocess

    subprocess.run(["curl", "-o", "/tmp/lotr.csv", "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"])


    def parse(graph, tuple: str):
        parts = [v.strip() for v in tuple.split(",")]
        source_node = parts[0]
        target_node = parts[1]
        time_stamp = int(parts[2])

        graph.add_vertex(time_stamp, source_node, vertex_type="Character")
        graph.add_vertex(time_stamp, target_node, vertex_type="Character")
        graph.add_edge(time_stamp, source_node, target_node, edge_type="Character_Co-occurence")


    with PyRaphtory.local() as ctx:
        lotr_spout = FileSpout("/tmp/lotr.csv")

        with ctx.new_graph() as graph:
            graph.load(Source(lotr_spout, GraphBuilder(parse)))

            df = (graph
                  .select(lambda vertex: Row(vertex.name(), vertex.degree()))
                  .to_df(["name", "degree"]))
            print(df)

            df = (graph
                  .execute(PyRaphtory.algorithms.generic.community.LPA[Long]())
                  .to_df(["name", "lpa_label"])
                  )
            print(df)

            df = (graph.execute(PageRank()).to_df(["name", "pagerank"]))
            print(df)

            df = (graph.execute(ConnectedComponents()).to_df(["name", "component"]))
            print(df)

            #  df = (graph.execute(TwoHopPaths()).to_df(["start", "middle", "end"]))
            #  print(df)

            df = (graph.execute(LocalTriangleCount()).to_df(["name", "triangles"]))
            print(df)

            df = (graph.execute(GlobalTriangleCount()).to_df(["triangles"]))
            print(df)

            df = (graph.execute(Degree()).to_df(["name", "in-degree", "out-degree", "degree"]))
            print(df)

            graph.select(lambda vertex: Row(vertex.name(), vertex.degree())).write_to_file("/tmp/test").wait_for_job()

            def accum_step(v, s):
                ac = s["max_time"]
                latest = v.latest_activity().time()
                ac += latest


            df2 = (graph
                   .set_global_state(lambda s: s.new_accumulator("max_time", 0, op=lambda a, b: max(a, b)))
                   .step(accum_step)
                   .global_select(lambda s: Row(s["max_time"].value()))
                   .to_df(["max_time"]))
            print(df2)

        with ctx.new_graph() as graph2:
            # can just call add_vertex, add_edge on graph directly without spout/builder
            start = perf_counter()
            with open("/tmp/lotr.csv") as f:
                for line in f:
                    parse(graph2, line)
            print(f"time taken to ingest: {perf_counter() - start}s")

            df = (graph2
                  .select(lambda vertex: Row(vertex.name(), vertex.degree()))
                  .to_df(["name", "degree"]))
            print(df)

            df2 = (graph2
                   .select(lambda v: Row(v.name(), v.latest_activity().time()))
                   .to_df(["name", "latest_time"]))
            print(df2)





