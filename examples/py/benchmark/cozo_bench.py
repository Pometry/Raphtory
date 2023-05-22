from benchmark_base import BenchmarkBase
import gzip
import csv
from tqdm import tqdm
from pycozo.client import Client

profiles_file = "data/soc-pokec-profiles.txt.gz"  # 1,632,803
relationships_file = "data/soc-pokec-relationships.txt.gz"  # 30,622,564
simple_profile_file = "data/simple-profiles.csv"
simple_relationship_file = "data/simple-relationships.csv"


class CozoDBBench(BenchmarkBase):
    def __init__(self):
        self.client = Client()

    def name(self):
        return "CozoDB"

    def setup(self):
        self.client.run("{:create user { code: Int }}")
        self.client.run("{:create friend { fr: Int, to: Int }}")
        self.client.run("""
            res[user] <~
                CsvReader(types: ['Int'],
                          url: 'file:///Users/haaroony/Documents/dev/raphtory/examples/py/benchmark/data/simple-profiles.csv',
                          has_headers: false)
            
            ?[code] :=
                res[code]
            
            :replace user {
                code: Int
            }
        """)
        self.client.run("""
            res[] <~
                CsvReader(types: ['Int', 'Int'],
                          url: 'file:///Users/haaroony/Documents/dev/raphtory/examples/py/benchmark/data/simple-relationships-50000.csv',
                          delimiter: '\t',
                          has_headers: false)
            ?[fr, to] :=
                res[fr, to]
            
            :replace friend { fr: Int, to: Int }
        """)

    def degree(self):
        return self.client.run("?[user_id, total_degree, out_degree, in_degree] <~ DegreeCentrality(*friend[])")

    def out_neighbours(self):
        return self.degree()

    def page_rank(self):
        return self.client.run("?[user_id, page_rank] <~ PageRank(*friend[])")

    def connected_components(self):
        return self.client.run("?[user_id, component] <~ ConnectedComponents(*friend[])")
