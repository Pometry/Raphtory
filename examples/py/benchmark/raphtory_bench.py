from benchmark_base import BenchmarkBase
import gzip
import csv
import raphtory
from tqdm import tqdm
from raphtory.algorithms import pagerank, weakly_connected_components

profiles_file = "data/soc-pokec-profiles.txt.gz"  # 1,632,803
relationships_file = "data/soc-pokec-relationships.txt.gz"  # 30,622,564
simple_profile_file = "data/simple-profiles.csv"
simple_relationship_file = "data/simple-relationships.csv"

class RaphtoryBench(BenchmarkBase):

    def __init__(self):
        self.graph = None

    def setup(self):
        # Load edges
        self.graph = raphtory.Graph()
        with gzip.open(relationships_file, 'rt') as f:
        #with open(simple_relationship_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in tqdm(reader, total=30622564):
                self.graph.add_edge(1, row[0], row[1], {})

    def degree(self):
        return list(self.graph.vertices().degree())

    def out_neighbours(self):
        return list([list(o) for o in self.graph.vertices().out_neighbours()])

    def page_rank(self):
        return pagerank(self.graph, iter_count=20)

    def connected_components(self):
        return weakly_connected_components(self.graph, iter_count=20)
