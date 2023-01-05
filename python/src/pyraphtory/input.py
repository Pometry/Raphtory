from pyraphtory.interop import ScalaClassProxy, GenericScalaProxy


class GraphBuilder(ScalaClassProxy):
    _classname = "com.raphtory.api.input.GraphBuilder"


class Type(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Type"


class Property(GenericScalaProxy):
    _classname = "com.raphtory.api.input.Property"


class ImmutableString(ScalaClassProxy, Property):
    _classname = "com.raphtory.api.input.ImmutableString"


class MutableString(ScalaClassProxy, Property):
    _classname = "com.raphtory.api.input.MutableString"


class MutableLong(ScalaClassProxy, Property):
    _classname = "com.raphtory.api.input.MutableLong"


class MutableDouble(ScalaClassProxy, Property):
    _classname = "com.raphtory.api.input.MutableDouble"


class MutableFloat(ScalaClassProxy, Property):
    _classname = "com.raphtory.api.input.MutableFloat"


class MutableBoolean(ScalaClassProxy, Property):
    _classname = "com.raphtory.api.input.MutableBoolean"


class Properties(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Properties"
