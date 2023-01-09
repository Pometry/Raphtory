from pyraphtory.interop import GenericScalaProxy, register, is_PyJObject
from pyraphtory.scala.implicits.numeric import Long, Double, Float, Int
from pyraphtory.scala.implicits.bounded import Bounded
from pyraphtory.scala.implicits.schemas import SchemaProviders


@register(name="Vertex")
class Vertex(GenericScalaProxy):
    """Wrapper for Raphtory vertex with setitem and getitem methods for working with vertex state"""

    _classname = "com.raphtory.api.analysis.visitor.Vertex"

    def message_vertex(self, vertex_id, data):
        super().message_vertex[SchemaProviders.get_schema(data)](vertex_id, data)

    def message_self(self, data):
        super().message_self[SchemaProviders.get_schema(data)](data)

    def message_out_neighbours(self, message):
        super().message_out_neighbours[SchemaProviders.get_schema(message)](message)

    def message_in_neighbours(self, message):
        super().message_in_neighbours[SchemaProviders.get_schema(message)](message)

    def message_all_neighbours(self, message):
        super().message_all_neighbours[SchemaProviders.get_schema(message)](message)
          
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
    _classname = "com.raphtory.api.analysis.graphstate.GraphState"
    """Wrapper for the global GraphState object which enables access to graphstate using getitem syntax"""
    def __getitem__(self, key):
        return self.apply(key)

    def new_int_max(self, *args, **kwargs):
        return super().new_max[Long, Bounded.long_bounds()](*args, **kwargs)

    def new_float_max(self, *args, **kwargs):
        return super().new_max[Double, Bounded.double_bounds()](*args, **kwargs)

    def new_int_min(self, *args, **kwargs):
        return super().new_min[Long, Bounded.long_bounds()](*args, **kwargs)

    def new_float_min(self, *args, **kwargs):
        return super().new_min[Double, Bounded.double_bounds()](*args, **kwargs)
