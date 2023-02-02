"""Algorithms that explore temporal graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.temporal."


class Ancestors(ScalaAlgorithm):
    _classname = _prefix + "Ancestors"


class Descendants(ScalaAlgorithm):
    _classname = _prefix + "Descendants"


class TemporalEdgeList(ScalaAlgorithm):
    _classname = _prefix + "TemporalEdgeList"


class TemporalNodeList(ScalaAlgorithm):
    _classname = _prefix + "TemporalNodeList"


