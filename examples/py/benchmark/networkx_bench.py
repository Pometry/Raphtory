from benchmark_base import BenchmarkBase
import networkx as nx
import gzip
import csv
from tqdm import tqdm
import random

profiles_file = "data/soc-pokec-profiles.txt.gz" # 1,632,803
relationships_file = "data/soc-pokec-relationships.txt.gz" # 30,622,564
simple_profile_file = "data/simple-profiles.csv"
simple_relationship_file = "data/simple-relationships.csv"

class NetworkXBench(BenchmarkBase):
    def __init__(self):
        self.graph = None

    def setup(self):
        self.graph = nx.DiGraph()
        with gzip.open(relationships_file, 'rt') as f:
        #with open(simple_relationship_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in tqdm(reader, total=30622564):
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