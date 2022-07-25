class Vertex(object):
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

    def in_degree(self):
        return self.v.in_degree()

    def out_degree(self):
        return self.v.out_degree()

    def degree(self):
        return self.v.degree()

    def out_neighbours(self):
        return self.v.out_neighbours()

    def message_outgoing_neighbours(self, msg):
        self.v.message_outgoing_neighbours(msg)

    def name(self, name_property="name"):
        return self.v.name(name_property)

    def neighbours(self):
        return self.v.neighbours()

    def message_vertex(self, id, msg):
        return self.v.message_vertex(id, msg)


class GraphState(object):
    def __init__(self, jvm_global_state):
        self.jvm_global_state = jvm_global_state

    def __setitem__(self, key, value):
        self.jvm_global_state.set_state_num(key, value)

    def __getitem__(self, key):
        return self.jvm_global_state.get_state_num(key)

    def numAdder(self, name: str, initial_value: int, retain_state: bool):
        self.jvm_global_state.numAdder(name, initial_value, retain_state)

