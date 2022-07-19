import traceback

from pyraphtory.vertex import Vertex


class Step(object):
    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass


class Iterate(object):

    def __init__(self, iterations: int, execute_messaged_only: bool):
        self.iterations = iterations
        self.execute_messaged_only = execute_messaged_only

    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass
