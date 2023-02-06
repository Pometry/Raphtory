"""Algorithms that explore temporal graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal."


class Ancestors(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "Ancestors"


class Descendants(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "Descendants"


class TemporalEdgeList(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "TemporalEdgeList"


class TemporalNodeList(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "TemporalNodeList"


