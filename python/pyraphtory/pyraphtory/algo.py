from pyraphtory.vertex import Vertex


class ConnectedComponents(object):
    def __init__(self):
        from pemja import findClass
        JConnectedComponents = findClass('com.raphtory.algorithms.generic.ConnectedComponents ')
        self.jvm_algo = JConnectedComponents()


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
