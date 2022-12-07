from pyraphtory.interop import ScalaClassProxy

class GraphBuilder(ScalaClassProxy):
    _classname = "com.raphtory.api.input.GraphBuilder"

class Type(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Type"

class ImmutableString(ScalaClassProxy):
    _classname = "com.raphtory.api.input.ImmutableString"

class MutableString(ScalaClassProxy):
    _classname = "com.raphtory.api.input.MutableString"

class MutableLong(ScalaClassProxy):
    _classname = "com.raphtory.api.input.MutableLong"

class MutableDouble(ScalaClassProxy):
    _classname = "com.raphtory.api.input.MutableDouble"

class MutableFloat(ScalaClassProxy):
    _classname = "com.raphtory.api.input.MutableFloat"

class MutableBoolean(ScalaClassProxy):
    _classname = "com.raphtory.api.input.MutableBoolean"

class Properties(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Properties"
