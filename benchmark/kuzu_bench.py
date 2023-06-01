from benchmark_base import BenchmarkBase
# Dont fail if not installed
try:
    import kuzu
except ImportError:
    pass


class KuzuBench(BenchmarkBase):

    def start_docker(self):
        image_name = 'python:3.10-bullseye'
        container_folder = '/app/data'
        exec_commands = [
            'pip install requests tqdm docker kuzu pandas numpy scipy',
            '/bin/bash -c "cd /app/data;python benchmark_driver.py --no-docker --bench k"'
        ]
        code, contents = super().start_docker(image_name, container_folder, exec_commands)
        return code, contents

    def shutdown(self):
        del self.conn
        del self.db

    def name(self):
        return "Kuzu"

    def run_query(self, query):
        print("Running query: " + query)
        res = self.conn.execute(query)
        if not res.is_success():
            raise Exception("Error running query")
        return res

    def setup(self):
        self.db = kuzu.Database('/tmp/testdb')
        self.conn = kuzu.Connection(self.db)
        self.run_query("CREATE NODE TABLE User(id INT64, PRIMARY KEY (id))")
        self.run_query("CREATE REL TABLE Follows(FROM User TO User)")
        self.run_query('COPY User FROM "data/simple-profiles.csv"')
        self.run_query('COPY Follows FROM "data/simple-relationships.csv" (DELIM="\t")')

    def degree(self):
        res = self.run_query('MATCH (a:User)-[f:Follows]->(b:User) RETURN a.id,COUNT(f)')
        df = res.get_as_df()

    def out_neighbours(self):
        self.conn.set_query_timeout(300000) # 300 seconds
        res = self.run_query('MATCH (u:User)-[:Follows]->(n)'
                             'RETURN u.id, COLLECT(n.id) AS out_neighbours')
        df = res.get_as_df()

    def page_rank(self):
        print("NOT IMPLEMENTED IN KUZU")

    def connected_components(self):
        print("NOT IMPLEMENTED IN KUZU")

    def __init__(self):
        self.conn = None
        self.db = None
