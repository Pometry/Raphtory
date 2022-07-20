import traceback
from typing import List

import cloudpickle as pickle
from pyraphtory.algo import Iterate, Step


class ProgressTracker(object):
    def __init__(self, jvm_tracker):
        self.jvm_tracker = jvm_tracker

    def wait_for_job(self):
        self.jvm_tracker.waitForJobInf()

    def job_id(self):
        self.jvm_tracker.getJobId()

    def inner_tracker(self):
        return self.jvm_tracker


class Table(object):

    def __init__(self, jvm_table):
        self.jvm_table = jvm_table

    def write_to_file(self, name: str):
        g = self.jvm_table.writeToFile(name)
        return ProgressTracker(g)


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

    def transform(self, algo):
        g = self.jvm_graph.transform(algo.jvm_algo)
        return TemporalGraph(g)

    def select(self, columns: List[str]):
        try:
            g = self.jvm_graph.pythonSelect(columns)
            return Table(g)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
