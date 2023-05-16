from benchmark_base import BenchmarkBase
# Load raphtory
import gzip
import csv
import raphtory
from tqdm import tqdm
import random

profiles_file = "data/soc-pokec-profiles.txt.gz" # 1,632,803
relationships_file = "data/soc-pokec-relationships.txt.gz" # 30,622,564

class RaphtoryBench(BenchmarkBase):

    def __init__(self):
        self.graph = raphtory.Graph()

    def setup(self):
        # Load edges
        with gzip.open(relationships_file, 'rt') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in tqdm(reader, total=30622564):
                self.graph.add_edge(1, row[0], row[1], {})

    def degree(self):
        self.graph

    def out_neighbours(self, id):
        pass

    def page_rank(self):
        pass

    def connected_components(self):
        pass
