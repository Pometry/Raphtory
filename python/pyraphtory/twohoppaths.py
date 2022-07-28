import traceback

from pyraphtory.vertex import Row

from pyraphtory.steps import Vertex, Iterate, Step, Explode
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


REQUEST_FIRST_HOP = 'request_first_hop'
REQUEST_SECOND_HOP = 'request_second_hop'
TWO_HOP_PATHS = 'two_hop_paths'
RESPONSE = 'response'

class TwoHopPathsStep(Step):
    def __init__(self, seeds: set[str] = set()):
        self.seeds = seeds

    def eval(self, v: Vertex):
        if (len(self.seeds) == 0) or (v.name() in self.seeds):
            v[TWO_HOP_PATHS] = list([])
            v.message_outgoing_neighbours({'req': REQUEST_FIRST_HOP, 'source': v.id()})

class TwoHopPathsIterate(Iterate):
    def __init__(self, iterations: int = 3, execute_messaged_only: bool = True):
        super().__init__(iterations, execute_messaged_only)

    def eval(self, v: Vertex):
        new_messages = v.message_queue()
        if len(new_messages):
            for msg in new_messages:
                match msg:
                    case msg if msg['req'] == REQUEST_FIRST_HOP:
                        source = msg['source']
                        neighbours = v.out_neighbours()
                        for neighbour in neighbours:
                            if neighbour != source:
                                v.message_vertex(neighbour, {'req': REQUEST_SECOND_HOP, 'source': source, 'first_hop': v.name()})
                    case msg if msg['req'] == REQUEST_SECOND_HOP:
                        first_hop = msg['first_hop']
                        source = msg['source']
                        v.message_vertex(source, {'req': RESPONSE, 'first_hop': first_hop, 'second_hop': v.name()})
                    case msg if msg['req'] == RESPONSE:
                        paths = set(v[TWO_HOP_PATHS])
                        newPath = (msg['first_hop'], msg['second_hop'])
                        paths.add(newPath)
                        v[TWO_HOP_PATHS] = tuple(paths)
                    case _:
                        pass

class TwoHopPathsExplode(Explode):
    def eval(self, r: Row):
        return_rows = []
        if len(r) > 1:
            if r[1] is not None:
                for hop_pair in r[1]:
                    return_rows.append( [r[0]] + list(hop_pair) )
        else:
            return_rows = [[r[0]]]
        return return_rows


class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            select_table = self.rg.at(32674) \
                .past() \
                .step(TwoHopPathsStep(seeds=set(['Gandalf']))) \
                .iterate(TwoHopPathsIterate()) \
                .select([TWO_HOP_PATHS])
            return select_table \
                .explode(TwoHopPathsExplode()) \
                .write_to_file("/tmp/pyraphtory_twohops")
                #


        except Exception as e:
            print(str(e))
            traceback.print_exc()
