from benchmark_base import BenchmarkBase
import pandas as pd
import multiprocessing

# Dont fail if not imported locally
try:
    import raphtory
    from tqdm import tqdm
    from raphtory.algorithms import pagerank, weakly_connected_components
except ImportError as e:
    print("IMPORT ERROR")
    print(e)
    print("Cannot continue. Exiting")
    import sys

    sys.exit(1)

simple_relationship_file = "data/simple-relationships.csv"


class RaphtoryBench(BenchmarkBase):
    def start_docker(self, **kwargs):
        image_name = "python:3.10-bullseye"
        container_folder = "/app/data"
        exec_commands = [
            "pip install raphtory requests tqdm pandas numpy docker",
            '/bin/bash -c "cd /app/data;python benchmark_driver.py --no-docker --bench r"',
        ]
        code, contents = super().start_docker(
            image_name, container_folder, exec_commands
        )
        return code, contents

    def shutdown(self):
        del self.graph

    def name(self):
        return "Raphtory"

    def __init__(self):
        self.graph = None

    def setup(self):
        # Load edges
        df = pd.read_csv(
            simple_relationship_file, delimiter="\t", header=None, names=["src", "dst"]
        )
        df["time"] = 1
        self.graph = raphtory.Graph.load_from_pandas(df, "src", "dst", "time")

    def degree(self):
        return self.graph.nodes.degree().collect()

    def out_neighbours(self):
        return self.graph.nodes.out_neighbours.collect()

    def page_rank(self):
        return pagerank(self.graph, iter_count=100)

    def connected_components(self):
        return weakly_connected_components(self.graph, iter_count=20)
