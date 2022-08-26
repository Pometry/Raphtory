from pyraphtory.interop import ScalaClassProxy


class FileSpout(ScalaClassProxy):
    _classname = "com.raphtory.spouts.FileSpout"


class IdentitySpout(ScalaClassProxy):
    _classname = "com.raphtory.spouts.IdentitySpout"


class StaticGraphSpout(ScalaClassProxy):
    _classname = "com.raphtory.spouts.StaticGraphSpout"


class WebSocketSpout(ScalaClassProxy):
    _classname = "com.raphtory.spouts.WebSocketSpout"
