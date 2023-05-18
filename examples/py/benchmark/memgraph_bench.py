# !pip install gqlalchemy
# docker run -it -p 7687:7687 -p 7444:7444 -p 3000:3000 -v mg_lib:/var/lib/memgraph memgraph/memgraph-platform
# http://localhost:3000/quick-connect has the memgraph lab!

profiles_file = "data/soc-pokec-profiles.txt.gz"  # 1,632,803
relationships_file = "data/soc-pokec-relationships.txt.gz"  # 30,622,564
simple_profile_file = "data/simple-profiles.csv"
simple_relationship_file = "data/simple-relationships.csv"


from benchmark_base import BenchmarkBase
from gqlalchemy import Memgraph

class MemgraphBench(BenchmarkBase):
    def __init__(self):
        self.graph = None

    def import_data(self):
        with open(simple_relationship_file, 'r') as file:
            for line in file:
                source, target = line.strip().split('\t')
                query = f"CREATE (n1:Node {{id: '{source}'}})-[:FOLLOWS]->(n2:Node {{id: '{target}'}})"
                self.graph.execute(query)

    def setup(self):
        self.graph = Memgraph(host='127.0.0.1', port=7687)
        # query = "MATCH (n) DETACH DELETE n"
        # self.graph.execute(query)
        self.import_data()

    def degree(self):
        query = "MATCH (n)-[f]-() RETURN n.id, COUNT(f);"
        result = self.graph.execute_and_fetch(query)
        return list(result)

    def out_neighbours(self):
        query = f"MATCH (n)-[:FOLLOWS]->(neighbor) RETURN n.id AS node_id, COLLECT(neighbor.id) AS neighbor_ids"
        return list(self.graph.execute_and_fetch(query))

    def page_rank(self):
        query = "CALL pagerank.get() YIELD node, rank RETURN node, rank;"
        return list(self.graph.execute_and_fetch(query))

    def connected_components(self):
        query = "CALL weakly_connected_components.get() YIELD node, component_id;"
        return list(self.graph.execute_and_fetch(query))
