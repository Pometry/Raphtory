"""Algorithms for finding motif counts in graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.generic.motif."


class GlobalClusteringCoefficient(ScalaAlgorithm):
    _classname = _prefix + "GlobalClusteringCoefficient"


class GlobalTriangleCount(ScalaAlgorithm):
    _classname = _prefix + "GlobalTriangleCount"


class LocalClusteringCoefficient(ScalaAlgorithm):
    _classname = _prefix + "LocalClusteringCoefficient"


class LocalTriangleCount(ScalaAlgorithm):
    _classname = _prefix + "LocalTriangleCount"


class SquareCount(ScalaAlgorithm):
    _classname = _prefix + "SquareCount"


class ThreeNodeMotifs(ScalaAlgorithm):
    _classname = _prefix + "ThreeNodeMotifs"
