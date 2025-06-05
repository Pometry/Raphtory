from benchmark_base import BenchmarkBase

# Dont fail if not installed
try:
    import kuzu
except ImportError:
    pass


class KuzuBench(BenchmarkBase):
    def start_docker(self):
        image_name = "python:3.10-bullseye"
        container_folder = "/app/data"
        exec_commands = [
            "pip install requests tqdm docker kuzu pandas numpy scipy",
            '/bin/bash -c "cd /app/data;python benchmark_driver.py --no-docker --bench k"',
        ]
        code, contents = super().start_docker(
            image_name, container_folder, exec_commands
        )
        return code, contents

    def shutdown(self):
        del self.conn
        del self.db

    def name(self):
        return "Kuzu"

    def run_query(self, query):
        print("Running query: " + query)
        res = self.conn.execute(query)
        return res

    def setup(self):
        self.db = kuzu.Database("/tmp/testdb")
        self.conn = kuzu.Connection(self.db)
        self.run_query("CREATE NODE TABLE User(id INT64, PRIMARY KEY (id))")
        self.run_query("CREATE REL TABLE Follows(FROM User TO User)")
        self.run_query('COPY User FROM "data/simple-profiles.csv"')
        self.run_query('COPY Follows FROM "data/simple-relationships.csv" (DELIM="\t")')

    def degree(self):
        res = self.run_query(
            "MATCH (a:User)-[f:Follows]->(b:User) RETURN a.id,COUNT(f)"
        )
        df = res.get_as_df()

    def out_neighbours(self):
        self.conn.set_query_timeout(600000)  # 600 seconds
        # query = "MATCH (u:User) RETURN MAX(u.id) AS max_user_id"
        # res = self.run_query(query)
        max_user_id = 1632803  # res.get_as_df().iloc[0]['max_user_id']
        batch_size = 100000
        for i in range(0, max_user_id, batch_size):
            start_id = i + 1
            end_id = min(i + batch_size, max_user_id + 1)
            query = f"MATCH (u:User) WHERE u.id >= {start_id} AND u.id < {end_id} WITH u MATCH (u)-[:Follows]->(n:User) RETURN u.id, COLLECT(n.id) AS out_neighbours"
            res = self.run_query(query)
            df = res.get_as_df()
        # query = "MATCH (u:User)-[:Follows]->(n:User) RETURN u.id, COUNT(n.id) AS out_neighbours"
        # res = self.run_query(query)
        # df = res.get_as_df()

    def page_rank(self):
        print("NOT IMPLEMENTED IN KUZU")

    def connected_components(self):
        print("NOT IMPLEMENTED IN KUZU")

    def __init__(self):
        self.conn = None
        self.db = None
