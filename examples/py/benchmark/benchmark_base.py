### Create a abstract base class with abstract methods for benchmarking graph tools
### This class is used by the benchmarking scripts to benchmark the graph tools
### The benchmarking scripts are located in the examples/py/benchmark directory

from abc import ABC, abstractmethod


class BenchmarkBase(ABC):

    @abstractmethod
    def start_docker(self):
        pass

    @abstractmethod
    def name(self):
        return self.name()

    @abstractmethod
    def __init__(self):
        pass

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