from pyraphtory.proxy import register
from pyraphtory.interop import GenericScalaProxy


@register(name="Vertex")
class Vertex(GenericScalaProxy):
    """Wrapper for Raphtory vertex with setitem and getitem methods for working with vertex state"""
    def __setitem__(self, key, value):
        self.set_state(key, value)

    def __contains__(self, item):
        return self.contains_state(item, True)

    def __getitem__(self, key):
        if key in self:
            return self.get_state(key, True)
        else:
            raise KeyError(f"State with {key=} does not exist.")


@register(name="GraphState")
class GraphState(GenericScalaProxy):
    """Wrapper for the global GraphState object which enables access to graphstate using getitem syntax"""
    def __getitem__(self, key):
        return self.apply(key)
