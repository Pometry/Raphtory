"""Algorithms that explore temporal graphs"""


from pyraphtory.interop import ScalaClassProxy, ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.temporal."


class Ancestors(ScalaClassProxy):
    _classname = _prefix + "Ancestors"


class Descendants(ScalaClassProxy):
    _classname = _prefix + "Descendants"


class TemporalEdgeList(ScalaClassProxy):
    _classname = _prefix + "TemporalEdgeList"


class TemporalNodeList(ScalaClassProxy):
    _classname = _prefix + "TemporalNodeList"


