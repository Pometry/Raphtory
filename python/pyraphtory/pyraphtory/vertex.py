class Vertex:
    def __init__(self, jvm_v):
        self.v = jvm_v

    def __setitem__(self, key, value):
        self.v.set_state(key, value)

    def __getitem__(self, key):
        self.v.get_state(key, False)

    def id(self):
        return self.v.ID()

    def message_all_neighbours(self, msg):
        self.v.message_all_neighbours(msg)

    def message_queue(self):
        return self.v.message_queue()

    def vote_to_halt(self):
        self.v.vote_to_halt()


class RaphtoryStep:
    pass


class Step(RaphtoryStep):
    def eval(self, v: Vertex):
        v['cclabel'] = v.id()
        v.message_all_neighbours(v.id())


class Iterate(RaphtoryStep):
    def __init__(self, iterations: int, execute_messaged_only: bool):
        self.iterations = iterations,
        self.execute_messaged_only = execute_messaged_only

    def eval(self, v: Vertex):
        label = min(v.message_queue())
        if label < v['cclabel']:
            v['cclabel'] = label
            v.message_all_neighbours(label)
        else:
            v.vote_to_halt()
