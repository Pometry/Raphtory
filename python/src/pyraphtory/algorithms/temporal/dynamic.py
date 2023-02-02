"""Algorithms for dynamics on temporal graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.temporal.dynamic."


class GenericTaint(ScalaAlgorithm):
    _classname = _prefix + "GenericTaint"


