"""
sample python script.
"""

from pyraphtory.api.input import *
from pyraphtory.api.sources import Source
from pyraphtory.api.scala.implicits.numeric import Long
from pyraphtory.api.spouts import FileSpout
from time import perf_counter
from pyraphtory.algorithms.pagerank import PageRank
from pyraphtory.algorithms.connectedcomponents import ConnectedComponents
from pyraphtory.algorithms.trianglecount import LocalTriangleCount, GlobalTriangleCount
# from pyraphtory.algorithms.twohoppaths import TwoHopPaths
from pyraphtory.algorithms.degree import Degree

if __name__ == "__main__":
    from pyraphtory.api.context import PyRaphtory
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
                  .step(lambda vertex: vertex.set_state("name",vertex.name()))\
                  .step(lambda vertex: vertex.set_state("degree",vertex.degree()))\
                  .select("name", "degree"))\
                  .to_df()
            print(df)

            df = (graph
                  .execute(PyRaphtory.algorithms.generic.community.LPA[Long]())
                  .to_df()
                  )
            print(df)

            df = (graph.execute(PageRank()).to_df())
            print(df)

            df = (graph.execute(ConnectedComponents()).to_df())
            print(df)

            #  df = (graph.execute(TwoHopPaths()).to_df())
            #  print(df)

            df = (graph.execute(LocalTriangleCount()).to_df())
            print(df)

            df = (graph.execute(GlobalTriangleCount()).to_df())
            print(df)

            df = (graph.execute(Degree()).to_df())
            print(df)

            graph.step(lambda vertex: vertex.set_state("name",vertex.name()))\
                 .step(lambda vertex: vertex.set_state("degree",vertex.degree()))\
                 .select("name", "degree").write_to_file("/tmp/test").wait_for_job()

            def accum_step(v, s):
                ac = s["max_time"]
                latest = v.latest_activity().time()
                ac += latest


            df2 = (graph
                   .set_global_state(lambda s: s.new_accumulator("max_time", 0, op=lambda a, b: max(a, b)))
                   .step(accum_step)
                   .global_select("max_time")
                   .to_df())
            print(df2)

        with ctx.new_graph() as graph2:
            # can just call add_vertex, add_edge on graph directly without spout/builder
            start = perf_counter()
            with open("/tmp/lotr.csv") as f:
                for line in f:
                    parse(graph2, line)
            print(f"time taken to ingest: {perf_counter() - start}s")

            df = (graph2
                  .step(lambda vertex: vertex.set_state("name",vertex.name()))\
                  .step(lambda vertex: vertex.set_state("degree",vertex.degree()))\
                  .select("name", "degree")\
                  .to_df())
            print(df)

            df2 = (graph2
                   .step(lambda v: v.set_state("name",v.name()))\
                   .step(lambda v: v.set_state("latest_time",v.latest_activity().time()))\
                   .select("name", "latest_time")
                   .to_df())
            print(df2)





