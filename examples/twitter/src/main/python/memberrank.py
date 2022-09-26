from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex
from dataclasses import dataclass
import pyraphtory.scala.collection
from functools import reduce

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
            match map(rawscorelist):
                case graph.edge:
                    if(indegree < 0.0): 
                        Score(negativescore = indegree)
                    else:
                        Score(positivescore = indegree)
                case _:
                    Score()

            totalraw = reduce(lambda x,y:(x[0] + y[0], x[1] + y[1]), rawscorelist)

            negativerawscore = reduce(lambda x,y: x, totalraw)
            positiverawscore = reduce(lambda x,y: y, totalraw)

            match map(queue):
                case (neighbourid, prscore):
                    edge = v.get_in_edge(neighbourid)
                    if (indegree < 0): 
                       newscore = Score(negativescore = indegree * prscore)
                    else: 
                       newscore = Score(positivescore = indegree * prscore)   
                case _:
                    newscore = Score()

            totalscore = reduce(lambda x,y:(x[0] + y[0], x[1] + y[1]), newscore)
            negativenewscore = reduce(lambda x,y: x, totalscore)
            positivenewscore = reduce(lambda x,y: y, totalscore)

            v.set_state('negativerawscore', negativerawscore)
            v.set_state('negativenewscore', negativenewscore)
            v.set_state('positiverawscore', positiverawscore)
            v.set_state('positivenewscore', positivenewscore)

        def tabularise(self, graph: TemporalGraph):
            def get_scores(v: Vertex):
                rows_found = Row(
                                v.get_property_or_else('name', v.id),
                                v.get_state_or_else('prlabel', -1),
                                v.get_state_or_else("negativeRawScore", 0.0),
                                v.get_state_or_else("positiveRawScore", 0.0),
                                v.get_state_or_else("negativeNewScore", 0.0),
                                v.get_state_or_else("positiveNewScore", 0.0)
                            )
                return rows_found
            return graph.select(lambda v: get_scores(v))
        