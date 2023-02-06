"""Algorithms for dynamics on temporal graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal.dynamic."


class GenericTaint(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "GenericTaint"


