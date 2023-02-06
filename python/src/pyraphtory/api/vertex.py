"""Python wrappers for the vertex api"""

from pyraphtory.interop import GenericScalaProxy, register
from pyraphtory.api.scala.implicits.schemas import SchemaProviders


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



