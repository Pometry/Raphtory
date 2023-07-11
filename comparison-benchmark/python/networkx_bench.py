from benchmark_base import BenchmarkBase
import csv
# Dont fail if not imported locally
try:
    import networkx as nx
except ImportError:
    pass


simple_relationship_file = "data/simple-relationships.csv"


class NetworkXBench(BenchmarkBase):
    def start_docker(self, **kwargs):
        image_name = 'python:3.10-bullseye'
        container_folder = '/app/data'
        exec_commands = [
            'pip install requests tqdm docker networkx pandas numpy scipy',
            '/bin/bash -c "cd /app/data;python benchmark_driver.py --no-docker --bench nx"'
        ]
        code, contents = super().start_docker(image_name, container_folder, exec_commands)
        return code, contents

    def shutdown(self):
        self.graph.clear()
        del self.graph

    def __init__(self):
        self.graph = None

    def name(self):
        return "NetworkX"

    def setup(self):
        self.graph = nx.DiGraph()
        with open(simple_relationship_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                self.graph.add_edge(int(row[0]), int(row[1]))

    def degree(self):
        return self.graph.degree()

    def out_neighbours(self):
        sizes = []
        for node in self.graph.nodes:
            out_neighbors = [edge[1] for edge in self.graph.edges if edge[0] == node]
            sizes.append(len(out_neighbors))
        return sizes

    def page_rank(self):
        return nx.pagerank(self.graph)

    def connected_components(self):
        return [len(comp) for comp in nx.connected_components(self.graph.to_undirected())]
