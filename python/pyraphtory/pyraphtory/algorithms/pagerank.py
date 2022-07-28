import traceback

from pyraphtory.steps import Vertex, Iterate, Step
from pyraphtory.builder import *
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph


PR_LABEL = 'prlabel'

class PGStep1(Step):
    def eval(self, v: Vertex):
        initLabel = 1.0
        v[PR_LABEL] = initLabel
        out_degree = v.out_degree()
        if out_degree > 0:
            msg = initLabel / out_degree
            v.message_outgoing_neighbours(msg)

class PGIterate1(Iterate):
    def __init__(self, iterations: int, execute_messaged_only: bool, damping_factor: float = 0.85):
        super().__init__(iterations, execute_messaged_only)
        self.damping_factor = damping_factor

    def eval(self, v: Vertex):
        current_label = v[PR_LABEL]
        queue = v.message_queue()
        summed_queue = sum(queue)
        new_label = (1 - self.damping_factor) + self.damping_factor * summed_queue
        v[PR_LABEL] = new_label

        out_degree = v.out_degree()

        if out_degree > 0:
            v.message_outgoing_neighbours(new_label / out_degree)

        if abs(new_label - current_label) < 0.00001:
            v.vote_to_halt()


class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            return self.rg.at(32674) \
                .past() \
                .step(PGStep1()) \
                .iterate(PGIterate1(iterations=100, execute_messaged_only=False)) \
                .select([PR_LABEL]) \
                .write_to_file("/tmp/pyraphtory_pr_label")
        except Exception as e:
            print(str(e))
            traceback.print_exc()


