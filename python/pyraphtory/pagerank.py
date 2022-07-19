import traceback

from pyraphtory.algo import Vertex, Iterate, Step
from pyraphtory.builder import *
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph

class LotrGraphBuilder(BaseBuilder):
    def __init__(self):
        super(LotrGraphBuilder, self).__init__()

    def parse_tuple(self, line: str):
        src_node, target_node, timestamp, *_ = line.split(",")

        src_id = self.assign_id(src_node)
        tar_id = self.assign_id(target_node)

        self.add_vertex(int(timestamp), src_id, [ImmutableProperty("name", src_node)], "Character")
        self.add_vertex(int(timestamp), tar_id, [ImmutableProperty("name", target_node)], "Character")
        self.add_edge(int(timestamp), src_id, tar_id, [], "Character Co-occurence")
        # print(f"add edge {src_node} {target_node}")

PR_LABEL = 'prlabel'

class PGStep1(Step):
    def eval(self, v: Vertex):
        initLabel = 1.0
        v[PR_LABEL] = (v.name(), initLabel)
        out_degree = v.out_degree()
        if out_degree > 0:
            msg = initLabel / out_degree
            v.message_outgoing_neighbours((v.name(), msg))
            if v.name() == "Isildur":
                print("SARUMAN OUT DEGREE "+str(out_degree)+" and MESSAGE "+str(msg))

class PGIterate1(Iterate):
    def __init__(self, iterations: int, execute_messaged_only: bool, damping_factor: float = 0.85):
        super().__init__(iterations, execute_messaged_only)
        self.damping_factor = damping_factor

    def eval(self, v: Vertex):
        current_label_s = v[PR_LABEL]
        current_label = current_label_s[1]
        queue_s = v.message_queue()
        queue = [x[1] for x in queue_s]
        # print(v.name())
        if v.name() == "Isildur":
            print(f'S-MAN queue size {len(queue)}, IN_DEG {v.in_degree()} OUT_DEG {v.out_degree()}')
            print(f'S-MAN messages {queue_s}')
        summed_queue = sum(queue)
        new_label = (1 - self.damping_factor) + self.damping_factor * summed_queue
        v[PR_LABEL] = (v.name(), new_label)

        out_degree = v.out_degree()
        abs_val = abs(new_label - current_label)

        if out_degree > 0:
            msg = new_label / out_degree
            v.message_outgoing_neighbours((v.name(), msg))
            if v.name() == "Isildur":
                print("S-MAN SUM_Q "+str(summed_queue)+", NEW_LL "+str(new_label)+"CURRENT_L "+str(current_label)+", ABS "+str(abs_val)+", OUTDEG "+str(out_degree)+", MSG "+str(msg))

        if abs_val < 0.00001:
            if v.name() == "Isildur":
                print("YOU SHALL NOT PASS")
            v.vote_to_halt()


class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            return self.rg.at(32674) \
                .past() \
                .step(PGStep1()) \
                .iterate(PGIterate1(iterations=100, execute_messaged_only=True)) \
                .select([PR_LABEL]) \
                .write_to_file("/tmp/pyraphtory_pr_label")
        except Exception as e:
            print(str(e))
            traceback.print_exc()


