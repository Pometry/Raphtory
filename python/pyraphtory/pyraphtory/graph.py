from pyraphtory.extra import Algorithm, Sink


class TemporalGraphConnection:
    def __init__(self, jvm_graph):
        self.jvm_graph = jvm_graph

    def at(self, time: int):
        g = self.jvm_graph.at(time)
        return TemporalGraphConnection(g)

    def past(self):
        g = self.jvm_graph.past()
        return TemporalGraphConnection(g)

    def execute(self, algo: Algorithm):
        g = self.jvm_graph.execute(algo.jvm_algo)
        return TemporalGraphConnection(g)

    def write_to(self, s: Sink):
        g = self.jvm_graph.writeTo(s.jvm_sink)
        return TemporalGraphConnection(g)

    def wait_for_job(self):
        self.jvm_graph.waitForJobInf()
