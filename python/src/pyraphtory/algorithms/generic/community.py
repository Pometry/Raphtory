"""Algorithms for community detection"""


from pyraphtory.interop import ScalaClassProxy, ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.community."


class LPA(ScalaClassProxyWithImplicits):
    _classname = _prefix + "LPA"


class SLPA(ScalaClassProxy):
    _classname = _prefix + "SLPA"
