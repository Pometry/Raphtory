from pyraphtory.extra import Sink
from pyraphtory.vertex import Step, Iterate
import dill as pickle


class TemporalGraphConnection:
    def __init__(self, jvm_graph):
        self.jvm_graph = jvm_graph

    def at(self, time: int):
        g = self.jvm_graph.at(time)
        return TemporalGraphConnection(g)

    def past(self):
        g = self.jvm_graph.past()
        return TemporalGraphConnection(g)

    # def execute(self, algo: Algorithm):
    #     g = self.jvm_graph.execute(algo.jvm_algo)
    #     return TemporalGraphConnection(g)

    def step(self, s: Step):
        step_bytes = pickle.dumps(s)
        g = self.jvm_graph.pythonStep(step_bytes)
        return TemporalGraphConnection(g)

    def iterate(self, i: Iterate):
        iterate_bytes = pickle.dumps(i)
        g = self.jvm_graph.pythonIterate(iterate_bytes, i.iterations, i.execute_messaged_only)
        return TemporalGraphConnection(g)

    def write_to(self, s: Sink):
        g = self.jvm_graph.writeTo(s.jvm_sink)
        return TemporalGraphConnection(g)

    def wait_for_job(self):
        self.jvm_graph.waitForJobInf()
