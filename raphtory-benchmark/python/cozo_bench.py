from benchmark_base import BenchmarkBase

try:
    from pycozo.client import Client
except ImportError:
    pass


class CozoDBBench(BenchmarkBase):
    def start_docker(self, **kwargs):
        image_name = "python:3.10-bullseye"
        container_folder = "/app/data"
        exec_commands = [
            "pip install requests docker pycozo[embedded,pandas]",
            '/bin/bash -c "cd /app/data;python benchmark_driver.py --no-docker --bench cozo"',
        ]
        code, contents = super().start_docker(
            image_name, container_folder, exec_commands
        )
        return code, contents

    def shutdown(self):
        self.client.close()

    def __init__(self):
        self.client = None
        self.docker = None
        self.container = None

    def name(self):
        return "CozoDB"

    def setup(self):
        self.client = Client()
        self.client.run("{:create user { code: Int }}")
        self.client.run("{:create friend { fr: Int, to: Int }}")
        self.client.run(
            """
            res[user] <~
                CsvReader(types: ['Int'],
                          url: 'file://./data/simple-profiles.csv',
                          has_headers: false)
            
            ?[code] :=
                res[code]
            
            :replace user {
                code: Int
            }
        """
        )
        self.client.run(
            """
            res[] <~
                CsvReader(types: ['Int', 'Int'],
                          url: 'file://./data/simple-relationships.csv',
                          delimiter: '\t',
                          has_headers: false)
            ?[fr, to] :=
                res[fr, to]
            
            :replace friend { fr: Int, to: Int }
        """
        )

    def degree(self):
        return self.client.run(
            "?[user_id, total_degree, out_degree, in_degree] <~ DegreeCentrality(*friend[])"
        )

    def out_neighbours(self):
        return self.degree()

    def page_rank(self):
        return self.client.run("?[user_id, page_rank] <~ PageRank(*friend[])")

    def connected_components(self):
        return self.client.run(
            "?[user_id, component] <~ ConnectedComponents(*friend[])"
        )
