from benchmark_base import BenchmarkBase

try:
    from gqlalchemy import Memgraph
except ImportError:
    pass


simple_profile_file = "data/simple-profiles.csv"
simple_relationship_file = "data/simple-relationships.csv"


class MemgraphBench(BenchmarkBase):
    def start_docker(self, **kwargs):
        image_name = "memgraph/memgraph-platform:latest"
        container_folder = "/app/data"
        exec_commands = [
            '/bin/bash -c "apt update && apt install -y libssl-dev"',
            '/bin/bash -c "python3 -m pip install gqlalchemy requests tqdm docker pandas"',
            '/bin/bash -c "cp -R /app/data/data /tmp/;chmod 777 -R /tmp/data"',
            '/bin/bash -c "cd /app/data;python3 benchmark_driver.py --no-docker --bench mem"',
        ]
        # ports = {
        #     '7444': '7444',
        #     '7687': '7687',
        #     '3000': '3000'
        # }
        code, contents = super().start_docker(
            image_name=image_name,
            container_folder=container_folder,
            exec_commands=exec_commands,
            # ports=ports,
        )
        return code, contents

    def shutdown(self):
        del self.graph

    def name(self):
        return "Memgraph"

    def __init__(self):
        self.graph = None

    def import_data(self):
        print("loading nodes")
        query = (
            'LOAD CSV FROM "/tmp/data/simple-profiles.csv" NO HEADER DELIMITER  "\t" AS row '
            "CREATE (n:Node {id: row[0]});"
        )
        self.graph.execute(query)
        print("Creating index")
        query = "CREATE INDEX ON :Node(id);"
        self.graph.execute(query)
        print("loading relationships")
        query = (
            'LOAD CSV FROM "/tmp/data/simple-relationships.csv" NO HEADER DELIMITER  "\t" AS row '
            "MATCH (n1:Node {id: row[0]}),  (n2:Node {id: row[1]}) CREATE (n1)-[:FOLLOWS]->(n2);"
        )
        self.graph.execute(query)

    def setup(self):
        self.graph = Memgraph(host="127.0.0.1", port=7687)
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
