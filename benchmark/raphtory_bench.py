from benchmark_base import BenchmarkBase
import csv
import multiprocessing

# Dont fail if not imported locally
try:
    import raphtory
    from tqdm import tqdm
    from raphtory.algorithms import pagerank, weakly_connected_components
except ImportError:
    pass

simple_relationship_file = "data/simple-relationships.csv"


class RaphtoryBench(BenchmarkBase):

    def start_docker(self, **kwargs):
        image_name = 'python:3.10-bullseye'
        container_folder = '/app/data'
        exec_commands = [
            'pip install raphtory requests tqdm pandas numpy docker',
            '/bin/bash -c "cd /app/data;python benchmark_driver.py --no-docker --bench r"'
        ]
        code, contents = super().start_docker(image_name, container_folder, exec_commands)
        return code, contents

    def shutdown(self):
        del self.graph

    def name(self):
        return "Raphtory"

    def __init__(self):
        self.graph = None

    def setup(self):
        # Load edges
        self.graph = raphtory.Graph(multiprocessing.cpu_count())
        with open(simple_relationship_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                self.graph.add_edge(1, row[0], row[1], {})

    def degree(self):
        return list(self.graph.vertices().degree())

    def out_neighbours(self):
        return list([list(o) for o in self.graph.vertices().out_neighbours()])

    def page_rank(self):
        return pagerank(self.graph, iter_count=100)

    def connected_components(self):
        return weakly_connected_components(self.graph, iter_count=20)
