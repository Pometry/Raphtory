"""Algorithms for finding motif counts in graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.generic.motif."


class GlobalClusteringCoefficient(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "GlobalClusteringCoefficient"


class GlobalTriangleCount(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "GlobalTriangleCount"


class LocalClusteringCoefficient(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "LocalClusteringCoefficient"


class LocalTriangleCount(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "LocalTriangleCount"


class SquareCount(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "SquareCount"


class ThreeNodeMotifs(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "ThreeNodeMotifs"
