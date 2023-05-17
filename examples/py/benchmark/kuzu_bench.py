from benchmark_base import BenchmarkBase
import kuzu

class KuzuBench(BenchmarkBase):

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
        self.run_query('COPY User FROM "data/nodes.csv"')
        self.run_query('COPY Follows FROM "data/rel.csv" (DELIM="\t")')

    def degree(self):
        res = self.run_query('MATCH (a:User)-[f:Follows]->(b:User) RETURN a.id,COUNT(f)')
        df = res.get_as_df()

    def out_neighbours(self):
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
