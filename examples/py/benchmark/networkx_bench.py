from benchmark_base import BenchmarkBase
import networkx as nx
import gzip
import csv
from tqdm import tqdm
import random

profiles_file = "data/soc-pokec-profiles.txt.gz" # 1,632,803
relationships_file = "data/soc-pokec-relationships.txt.gz" # 30,622,564

class NetworkXBench(BenchmarkBase):
    def __init__(self):
        self.graph = nx.DiGraph()

    def setup(self):
        with gzip.open(relationships_file, 'rt') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in tqdm(reader, total=30622564):
                self.graph.add_edge(int(row[0]), int(row[1]))

    def degree(self):
        self.graph.degree()

    def out_neighbours(self, id):
        len([n for n in self.graph.neighbors(id)])

    def page_rank(self):
        nx.pagerank(self.graph)

    def connected_components(self):
        len([len(comp) for comp in nx.connected_components(self.graph)])