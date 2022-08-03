from pyraphtory.proxy import GenericScalaProxy


class Vertex(GenericScalaProxy):
    def __setitem__(self, key, value):
        self.set_state(key, value)

    def __getitem__(self, key):
        return self.get_state(key, True)


class GraphState(GenericScalaProxy):
    def __setitem__(self, key, value):
        self.set_state_num(key, value)

    def __getitem__(self, key):
        return self.get_state_num(key)
