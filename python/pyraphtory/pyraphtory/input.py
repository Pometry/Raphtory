from pyraphtory.interop import ScalaClassProxy

class GraphBuilder(ScalaClassProxy):
    _classname = "com.raphtory.api.input.GraphBuilder"

class Type(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Type"

class StringProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.StringProperty"

class ImmutableProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.ImmutableProperty"

class LongProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.LongProperty"

class DoubleProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.DoubleProperty"

class FloatProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.FloatProperty"

class BooleanProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.BooleanProperty"

class Properties(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Properties"
