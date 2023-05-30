from benchmark_base import BenchmarkBase
# Dont fail if not imported locally
try:
    from neo4j import GraphDatabase
except ImportError:
    pass


def import_data(tx):
    tx.run("""
    LOAD CSV FROM 'file:///data2/data/simple-relationships.csv' AS row
    FIELDTERMINATOR '\t'
    WITH row[0] AS source, row[1] AS target
    MERGE (n1:Node {id: source})
    MERGE (n2:Node {id: target})
    MERGE (n1)-[:FOLLOWS]->(n2)
    """)


def create_graph_projection(tx):
    tx.run("""
        CALL gds.graph.project.cypher(
        'social',
        'MATCH (n:Node) RETURN id(n) AS id',
        'MATCH (n:Node)-[r:FOLLOWS]->(m:node) RETURN id(n) AS source, id(m) AS target')
        YIELD
          graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels
          """)


def query_degree(tx):
    result = tx.run("""
        MATCH p=(n:Node)-[r:FOLLOWS]->() RETURN n.id, COUNT(p)
        """)
    return list(result)


def get_out_neighbors(tx):
    result = tx.run("""
        MATCH p=(n:Node)-[:FOLLOWS]->(neighbor)
        RETURN n.id, COUNT(p)
    """)
    return list(result)


def run_pagerank(tx):
    result = tx.run("""CALL gds.pageRank.stream("social")""")
    return list(result)


def run_connected_components(tx):
    result = tx.run("""
        CALL gds.wcc.stream("social")
    """)
    return list(result)


class Neo4jBench(BenchmarkBase):
    def start_docker(self, **kwargs):
        image_name = 'neo4j:5.8.0'
        container_folder = '/var/lib/neo4j/import/data2/'
        envs = {
            'NEO4J_AUTH': 'neo4j/password',
            'NEO4J_PLUGINS': '["graph-data-science"]'
        }
        # ports = {
        #     '7474': '7474',
        #     '7687': '7687'
        # }
        exec_commands = [
            '/bin/bash -c "apt update && apt install python3-pip -y"',
            '/bin/bash -c "python3 -m pip install neo4j requests tqdm pandas numpy docker"',
            '/bin/bash -c "cd /var/lib/neo4j/import/data2/; python3 benchmark_driver.py --no-docker --bench neo"',
        ]
        # image_path = 'DockerFiles/pyneo' image_path ports
        code, contents = super().start_docker(image_name=image_name, container_folder=container_folder,
                                              exec_commands=exec_commands, envs=envs, wait=35)
        return code, contents

    def shutdown(self):
        self.driver.close()

    def __init__(self):
        self.driver = None

    def name(self):
        return "Neo4j"

    def setup(self):
        uri = "bolt://localhost:7687"
        username = "neo4j"
        password = "password"
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.execute_write(import_data)
        self.execute_write(create_graph_projection)

    def execute_read(self, query):
        x = None
        with self.driver.session() as session:
            x = session.execute_read(query)
        return x

    def execute_write(self, query):
        with self.driver.session() as session:
            session.execute_write(query)

    def degree(self):
        return self.execute_read(query_degree)

    def out_neighbours(self):
        return self.execute_read(get_out_neighbors)

    def page_rank(self):
        return self.execute_read(run_pagerank)

    def connected_components(self):
        return self.execute_read(run_connected_components)
