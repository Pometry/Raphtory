from benchmark_base import BenchmarkBase
from neo4j import GraphDatabase


# docker run --publish 7474:7474 --publish 7687:7687
# --volume /Users/haaroony/Documents/dev/raphtory/examples/py/benchmark/neo:/data
# --volume /Users/haaroony/Documents/dev/raphtory/examples/py/benchmark/data:/var/lib/neo4j/import/data2
# --env NEO4J_AUTH=neo4j/password
# --env NEO4J_PLUGINS='["graph-data-science"]'
# neo4j

def import_data(tx):
    tx.run("""
    LOAD CSV FROM 'file:///data2/rel.csv' AS row
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
        'MATCH (n:Node)-[r:FOLLOWs]->(m:node) RETURN id(n) AS source, id(m) AS target')
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
    def __init__(self):
        uri = "bolt://localhost:7687"
        username = "neo4j"
        password = "password"
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.setup()

    def setup(self):
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
