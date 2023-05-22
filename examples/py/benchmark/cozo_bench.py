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
        local_folder = os.path.abspath(os.getcwd())
        container_folder = '/app/data'
        volumes = {local_folder: {'bind': container_folder, 'mode': 'ro'}}
        print('Running Docker container & benchmark...')
        self.container = self.docker.containers.run(
            image_name,
            volumes=volumes,
            detach=True,
            tty=True,
        )
        print('Waiting for container to finish pip setup...')
        exec_command = self.container.exec_run('pip install requests docker pycozo[embedded,pandas]')
        if exec_command.exit_code != 0:
            print('Error installing pip packages')
            print(exec_command.output.decode('utf-8'))
            self.container.stop()
            self.container.remove()
            return exec_command.exit_code, exec_command.output.decode('utf-8')
        print("Completed pip setup, running benchmark...")
        exec_command = self.container.exec_run('/bin/bash -c "cd /app/data;python benchmark_driver.py --bench cozo --save True"') #
        if exec_command.exit_code != 0:
            print('Error running packages')
            print(exec_command.output.decode('utf-8'))
            self.container.stop()
            self.container.remove()
            return exec_command.exit_code, exec_command.output.decode('utf-8')
        print('Benchmark completed, retrieving container logs...')
        file_path = '/tmp/bench-*.csv'
        file_contents = self.container.exec_run(['/bin/bash', '-c', f'cat {file_path}']).output.decode('utf-8').strip()
        # Remove the container
        print('Removing container...')
        self.container.stop()
        self.container.remove()
        # Return the exit code and logs
        return exec_command.exit_code, file_contents

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
