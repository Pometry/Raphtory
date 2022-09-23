from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex
from dataclasses import dataclass
import pyraphtory.scala.collection

@dataclass(frozen=True)
class Score(negativescore = 0.0, positivescore = 0.0):
    def plus(self, that):
        negative = self.negativescore + that.negativescore
        positive = self.positivescore + that.positivescore
    
        Score(negative,positive)


class MemberRank(PyAlgorithm):
    def __init__(self):
        pass

    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def init(v: Vertex):
            prvalue = v.get_state('prlabel')
            v.message_out_neighbours(v.id, prvalue)
            

        def step(v: Vertex):
            queue = v.message_queue
            indegree = v.in_degree.float()
            rawscorelist = v.in_edges()
            match rawscorelist.split():
                case graph.edge:
                    if(indegree < 0.0): 
                        Score(negativescore = indegree)
                    else:
                        Score(positivescore = indegree)
                case _:
                    Score()
        