### Create a abstract base class with abstract methods for benchmarking graph tools
### This class is used by the benchmarking scripts to benchmark the graph tools
### The benchmarking scripts are located in the examples/py/benchmark directory

from abc import ABC, abstractmethod
import docker
import os


class BenchmarkBase(ABC):

    def start_docker(self, image_name, container_folder, exec_commands):
        print('Creating Docker client...')
        self.docker = docker.from_env()

        print('Pulling Docker image...')
        self.docker.images.pull(image_name)

        print('Defining volumes...')
        local_folder = os.path.abspath(os.getcwd())
        volumes = {local_folder: {'bind': container_folder, 'mode': 'ro'}}

        print('Running Docker container & benchmark...')
        self.container = self.docker.containers.run(
            image_name,
            volumes=volumes,
            detach=True,
            tty=True,
        )
        try:
            for cmd in exec_commands:
                print(f'Running command {cmd}...')
                _, stream = self.container.exec_run(cmd, stream=True)
                for data in stream:
                    print(data.decode(), end='')
                print()
                # print(exec_command)
                # if exec_command.exit_code != 0:
                #     print(f'Error running command')
                #     print(exec_command.output.decode('utf-8'))
                #     self.container.stop()
                #     self.container.remove()
                #     return exec_command.exit_code, exec_command.output.decode('utf-8')
                print("Completed command...")
        except:
            print('Error running command')
            self.container.stop()
            self.container.remove()
            return 1, 'Error running command'

        print('Benchmark completed, retrieving results...')
        file_path = '/tmp/bench-*.csv'
        file_contents = self.container.exec_run(['/bin/bash', '-c', f'cat {file_path}']).output.decode('utf-8').strip()

        print('Removing container...')
        self.container.stop()
        self.container.remove()

        return 0, file_contents

    @abstractmethod
    def name(self):
        return ""

    @abstractmethod
    def __init__(self):
        self.container = None
        self.docker = None

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def degree(self):
        pass

    @abstractmethod
    def out_neighbours(self):
        pass

    @abstractmethod
    def page_rank(self):
        pass

    @abstractmethod
    def connected_components(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass
