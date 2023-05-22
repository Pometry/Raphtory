from benchmark_base import BenchmarkBase
from pycozo.client import Client
import docker
import os


class CozoDBBench(BenchmarkBase):
    def start_docker(self):
        print('Creating Docker client...')
        self.docker = docker.from_env()
        print('Pulling Docker image...')
        image_name = 'python:3.10-bullseye'
        self.docker.images.pull(image_name)
        print('Defining volumes...')
        local_folder = os.path.abspath(os.getcwd()) + '/data'
        container_folder = '/app/data'
        volumes = {local_folder: {'bind': container_folder, 'mode': 'ro'}}
        print('Running Docker container & benchmark...')
        self.container = self.docker.containers.run(
            image_name,
            command='pip install "pycozo[embedded,requests,pandas]" '
                    '&& python /app/data/benchmark_driver.py --bench cozo',
            volumes=volumes,
            detach=True
        )
        print('Waiting for container to finish...')
        exit_code = self.container.wait()['StatusCode']
        print('Retrieving container logs...')
        logs = self.container.logs().decode('utf-8')
        # Remove the container
        print('Removing container...')
        self.container.remove()
        # Return the exit code and logs
        return exit_code, logs

    def shutdown(self):
        self.client.close()

    def __init__(self):
        self.client = Client()
        self.docker = None
        self.container = None

    def name(self):
        return "CozoDB"

    def setup(self):
        self.client.run("{:create user { code: Int }}")
        self.client.run("{:create friend { fr: Int, to: Int }}")
        self.client.run("""
            res[user] <~
                CsvReader(types: ['Int'],
                          url: 'file://./data/simple-profiles.csv',
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
                          url: 'file://./data/simple-relationships.csv',
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
