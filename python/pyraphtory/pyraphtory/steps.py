from typing import Any, List

from pyraphtory.vertex import Vertex, GraphState, Row


class Step(object):
    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass


class StepState(object):
    def eval_from_jvm(self, jvm_vertex, jvm_graph_state):
        self.eval(Vertex(jvm_vertex), GraphState(jvm_graph_state))

    def eval(self, v, s):
        pass


class State(object):
    def eval_from_jvm(self, jvm_graph_state):
        self.eval(GraphState(jvm_graph_state))

    def eval(self, s):
        pass


class GlobalSelect(object):
    def eval_from_jvm(self, jvm_graph_state) -> List[Any]:
        return self.eval(GraphState(jvm_graph_state))

    def eval(self, gs) -> List[Any]:
        pass


class NumAdder(State):
    def __init__(self, name: str, initial_value: int = 0, retain_state: bool = False):
        super(NumAdder, self).__init__()
        self.name = name
        self.initial_value = initial_value
        self.retain_state = retain_state

    def eval(self, s: GraphState):
        s.numAdder(self.name, self.initial_value, self.retain_state)


class Iterate(object):
    def __init__(self, iterations: int, execute_messaged_only: bool):
        self.iterations = iterations
        self.execute_messaged_only = execute_messaged_only

    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass

class Explode(object):
    def eval_from_jvm(self, jvm_row):
        return self.eval(Row(jvm_row))

    def eval(self, r):
        pass

