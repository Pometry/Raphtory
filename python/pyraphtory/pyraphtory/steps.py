import traceback

from pyraphtory.vertex import Vertex, GlobalState


class Step(object):
    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass


class StepState(object):
    def eval_from_jvm(self, jvm_vertex, jvm_global_state):
        self.eval(Vertex(jvm_vertex), GlobalState(jvm_global_state))

    def eval(self, v, s):
        pass


class State(object):
    def eval_from_jvm(self, jvm_global_state):
        self.eval(GlobalState(jvm_global_state))

    def eval(self, s):
        pass


class NumAdder(State):
    def __init__(self, name: str, initial_value: int = 0, retain_state: bool = False):
        super(NumAdder, self).__init__()
        self.name = name
        self.initial_value = initial_value
        self.retain_state = retain_state

    def eval(self, s: GlobalState):
        s.numAdder(self.name, self.initial_value, self.retain_state)



class Iterate(object):

    def __init__(self, iterations: int, execute_messaged_only: bool):
        self.iterations = iterations
        self.execute_messaged_only = execute_messaged_only

    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass
