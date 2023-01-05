from pyraphtory.interop import ScalaClassProxy


"""Spouts"""


class Spout:
    """Base class for Raphtory spouts."""
    pass


class FileSpout(ScalaClassProxy, Spout):
    _classname = "com.raphtory.spouts.FileSpout"


class IdentitySpout(ScalaClassProxy, Spout):
    _classname = "com.raphtory.spouts.IdentitySpout"


class StaticGraphSpout(ScalaClassProxy, Spout):
    _classname = "com.raphtory.spouts.StaticGraphSpout"


class WebSocketSpout(ScalaClassProxy, Spout):
    _classname = "com.raphtory.spouts.WebSocketSpout"
