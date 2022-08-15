import inspect
from typing import Any, List

from pyraphtory.vertex import Vertex, GraphState
from pyraphtory.interop import to_jvm, to_python, FunctionWrapper





class Step(FunctionWrapper):

    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        self._fun(v)


class StepState(FunctionWrapper):
    def eval_from_jvm(self, jvm_vertex, jvm_graph_state):
        self.eval(Vertex(jvm_vertex), GraphState(jvm_graph_state))

    def eval(self, v, s):
        self._fun(v, s)


class State(FunctionWrapper):
    def eval_from_jvm(self, jvm_graph_state):
        self.eval(GraphState(jvm_graph_state))

    def eval(self, s):
        self._fun(s)


class GlobalSelect(FunctionWrapper):
    def eval_from_jvm(self, jvm_graph_state) -> List[Any]:
        return to_jvm(self.eval(GraphState(jvm_graph_state)))

    def eval(self, gs) -> List[Any]:
        return self._fun(gs)


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
