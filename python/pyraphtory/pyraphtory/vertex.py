from pyraphtory.proxy import GenericScalaProxy
from collections.abc import Mapping


class Vertex(GenericScalaProxy):
    def __setitem__(self, key, value):
        self.set_state(key, value)

    def __contains__(self, item):
        return self.contains_state(item, True)

    def __getitem__(self, key):
        if key in self:
            return self.get_state(key, True)
        else:
            raise KeyError(f"State with {key=} does not exist.")


class GraphState(GenericScalaProxy):
    def __setitem__(self, key, value):
        self.set_state_num(key, value)

    def __getitem__(self, key):
        return self.get_state_num(key)
