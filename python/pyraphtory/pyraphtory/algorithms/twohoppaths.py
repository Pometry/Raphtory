from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex

REQUEST_FIRST_HOP = 'request_first_hop'
REQUEST_SECOND_HOP = 'request_second_hop'
TWO_HOP_PATHS = 'two_hop_paths'
RESPONSE = 'response'


class TwoHopPaths(PyAlgorithm):
    def __init__(self, seeds: set[str] = set()):
        self.seeds = seeds

    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step1(v: Vertex):
            if (len(self.seeds) == 0) or (v.name() in self.seeds):
                v[TWO_HOP_PATHS] = list([])
                v.message_out_neighbours({'req': REQUEST_FIRST_HOP, 'source': v.id()})

        def iterate1(v: Vertex):
            new_messages = v.message_queue()
            if new_messages:
                for msg in new_messages:
                    if msg['req'] == REQUEST_FIRST_HOP:
                        source = msg['source']
                        neighbours = v.out_neighbours()
                        for neighbour in neighbours:
                            if neighbour != source:
                                v.message_vertex(neighbour, {'req': REQUEST_SECOND_HOP, 'source': source, 'first_hop': v.name()})
                    elif msg['req'] == REQUEST_SECOND_HOP:
                        first_hop = msg['first_hop']
                        source = msg['source']
                        v.message_vertex(source, {'req': RESPONSE, 'first_hop': first_hop, 'second_hop': v.name()})
                    elif msg['req'] == RESPONSE:
                        paths = set(v[TWO_HOP_PATHS])
                        newPath = (msg['first_hop'], msg['second_hop'])
                        paths.add(newPath)
                        v[TWO_HOP_PATHS] = tuple(paths)
                    else:
                        pass

        return graph.step(step1).iterate(iterate1, 3, True)

    def tabularise(self, graph: TemporalGraph) -> Table:
        def explode(r: Row):
            return_rows = []
            if len(r.get_values()) > 1:
                if r.get(1) is not None:
                    for hop_pair in r.get(1):
                        return_rows.append(Row(r.get(0), *hop_pair))
            else:
                return_rows = []
            return return_rows
        return graph.select(lambda v: Row(v.name(), v[TWO_HOP_PATHS])).explode(explode)
