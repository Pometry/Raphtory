from pyraphtory.interop import logger, assign_id, GenericScalaProxy, ScalaClassProxy


class Type(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Type"


class StringProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.StringProperty"


class ImmutableProperty(ScalaClassProxy):
    _classname = "com.raphtory.api.input.ImmutableProperty"


class Properties(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Properties"

class Source(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Source"



