import traceback

import cloudpickle as pickle

from pyraphtory.algo import Iterate, Step
from pyraphtory.extra import Sink


class TemporalGraph(object):
    def __init__(self, jvm_graph):
        self.jvm_graph = jvm_graph

    def at(self, time: int):
        g = self.jvm_graph.at(time)
        return TemporalGraph(g)

    def past(self):
        g = self.jvm_graph.past()
        return TemporalGraph(g)

    def step(self, s: Step):
        try:
            step_bytes = pickle.dumps(s)
            g = self.jvm_graph.pythonStep(step_bytes)
            return TemporalGraph(g)
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def iterate(self, i: Iterate):
        try:
            iterate_bytes = pickle.dumps(i)
            g = self.jvm_graph.pythonIterate(iterate_bytes, int(i.iterations), bool(i.execute_messaged_only))
            return TemporalGraph(g)
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def write_to(self, s: Sink):
        g = self.jvm_graph.writeTo(s.jvm_sink)
        return TemporalGraph(g)

    def wait_for_job(self):
        self.jvm_graph.waitForJobInf()
