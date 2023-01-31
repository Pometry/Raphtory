"""Algorithms for community detection"""


from pyraphtory.interop import ScalaClassProxy, ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.temporal.community."


class MultilayerLPA(ScalaClassProxy):
    _classname = _prefix + "MultilayerLPA"



