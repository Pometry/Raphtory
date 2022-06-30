from abc import ABC


class Algorithm(ABC):

    def __init__(self, jvm_algo):
        self.jvm_algo = jvm_algo


class ConnectedComponents(Algorithm):
    def __init__(self, jvm_algo):
        super().__init__(jvm_algo)


class Sink:
    def __init__(self, jvm_sink):
        self.jvm_sink = jvm_sink
