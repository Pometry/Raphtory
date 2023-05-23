from benchmark_base import BenchmarkBase
import graph_tool.all as gt
import csv

simple_profile_file = "data/simple-profiles.csv"
simple_relationship_file = "data/simple-relationships.csv"


class GraphToolBench(BenchmarkBase):
    def start_docker(self):
        pass

    def shutdown(self):
        self.graph.clear()

    def __init__(self):
        self.graph = None

    def name(self):
        return "GraphTool"

    def setup(self):
        self.graph = gt.Graph()
        # with gzip.open(relationships_file, 'rt') as f:
        with open(simple_relationship_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:  # , total=30622564):
                self.graph.add_edge(int(row[0]), int(row[1]))

    def degree(self):
        self.graph.degree_property_map('total').get_array()

    def out_neighbours(self):
        [len(list(v.out_neighbours())) for v in self.graph.vertices()]

    def page_rank(self):
        pr = gt.pagerank(self.graph)

    def connected_components(self):
        comp, hist = gt.label_components(self.graph)
        size_comp = len(list(comp))
        size_hist = len(hist)
