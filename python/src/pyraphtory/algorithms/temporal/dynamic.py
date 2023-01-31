"""Algorithms for dynamics on temporal graphs"""


from pyraphtory.interop import ScalaClassProxyWithImplicits, ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal.dynamic."


class GenericTaint(ScalaClassProxy):
    _classname = _prefix + "GenericTaint"


