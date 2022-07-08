class Vertex:
    def __init__(self, jvm_v):
        self.v = jvm_v

    def __setitem__(self, key, value):
        self.v.set_state(key, value)

    def __getitem__(self, key):
        return self.v.get_state(key, True)

    def id(self):
        return self.v.ID()

    def message_all_neighbours(self, msg):
        self.v.message_all_neighbours(msg)

    def message_queue(self):
        return self.v.message_queue()

    def vote_to_halt(self):
        self.v.vote_to_halt()


class Step(object):
    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass


class Iterate(object):

    def __init__(self, iterations: int, execute_messaged_only: bool):
        self.iterations = iterations
        self.execute_messaged_only = execute_messaged_only

    def eval_from_jvm(self, jvm_vertex):
        self.eval(Vertex(jvm_vertex))

    def eval(self, v):
        pass
