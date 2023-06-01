import os
import subprocess
import time

from benchmark_base import BenchmarkBase
import pandas as pd
import csv

# Dont fail if not imported locally
try:
    from neo4j import GraphDatabase
except ImportError:
    pass


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


def execute_bash_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.decode('utf-8'), stderr.decode('utf-8')


def write_array_to_csv(arr, file_path):
    with open(file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter='\t')
        writer.writerows(arr)


def modify_data():
    print("Generating data...")
    file_dir = os.path.abspath(os.getcwd()) + '/data/'
    print("File dir: ", file_dir)
    if 'simple-profiles-header-neo4j.csv' not in os.listdir(file_dir):
        print("Generating node header")
        write_array_to_csv([['node:ID', 'name']], file_dir + 'simple-profiles-header-neo4j.csv')

        print("Generating relationship header")
        write_array_to_csv([['node:START_ID', 'node:END_ID', ':TYPE']],
                           file_dir + 'simple-relationships-headers-neo4j.csv')

        print("Generating node data")
        df = pd.read_csv(file_dir + 'simple-profiles.csv', sep='\t', header=None)
        df['copy'] = df[0].copy()
        df.to_csv(file_dir + 'simple-profiles-neo4j.csv', index=None, header=None, sep='\t')

        print("Generating relationship data")
        df = pd.read_csv(file_dir + 'simple-relationships.csv', sep='\t', header=None)
        df['type'] = 'FOLLOWS'
        df.to_csv(file_dir + 'simple-relationships-neo4j.csv', sep='\t', index=None, header=None)
        print("Done")


def import_data():
    return execute_bash_command("neo4j-admin database import full --overwrite-destination --delimiter='TAB' "
                         "--nodes=/var/lib/neo4j/import/data2/data/simple-profiles-header-neo4j.csv,"
                         "/var/lib/neo4j/import/data2/data/simple-profiles-neo4j.csv"
                         "--relationships=/var/lib/neo4j/import/data2/data/simple-relationships-headers-neo4j.csv,"
                         "/var/lib/neo4j/import/data2/data/simple-relationships-neo4j.csv neo4j")
    # tx.run("""
    # LOAD CSV FROM 'file:///data2/data/simple-relationships.csv' AS row
    # FIELDTERMINATOR '\t'
    # WITH row[0] AS source, row[1] AS target
    # MERGE (n1:Node {id: source})
    # MERGE (n2:Node {id: target})
    # MERGE (n1)-[:FOLLOWS]->(n2)
    # """)


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
            # '/bin/bash -c "neo4j start"',
            # '/bin/bash -c "sleep 15"',
            '/bin/bash -c "cd /var/lib/neo4j/import/data2/; python3 benchmark_driver.py --no-docker --bench neo"',
        ]
        # image_path = 'DockerFiles/pyneo' image_path ports
        code, contents = super().start_docker(image_name=image_name, container_folder=container_folder,
                                              exec_commands=exec_commands, envs=envs, wait=35,
                                              start_cmd='tail -f /dev/null')
        return code, contents

    def shutdown(self):
        self.driver.close()

    def __init__(self):
        self.driver = None
        modify_data()

    def name(self):
        return "Neo4j"

    def setup(self):
        uri = "bolt://localhost:7687"
        username = "neo4j"
        password = "neo4j"
        print("Logging into neo4j")
        # self.driver = GraphDatabase.driver(uri, auth=(username, password))
        print("Importing data")
        stout, sterr = import_data()
        print("status: ", stout)
        print("Restarting neo4j")
        print("status: ", stout)
        stout, sterr = execute_bash_command('export NEO4J_PLUGINS=\'["graph-data-science"]\';neo4j start')
        print("status: ", stout)
        time.sleep(15)
        print("Creating graph projection")
        # change user password on neo4j login
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.execute_write("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'password'")
        self.execute_write(create_graph_projection)
        print("Done")

    def execute_read(self, query):
        with self.driver.session() as session:
            return session.execute_read(query)

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
