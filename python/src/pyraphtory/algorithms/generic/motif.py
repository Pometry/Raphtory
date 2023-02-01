"""Algorithms for finding motif counts in graphs"""


from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.generic.motif."


class GlobalClusteringCoefficient(ScalaClassProxy):
    _classname = _prefix + "GlobalClusteringCoefficient"


class GlobalTriangleCount(ScalaClassProxy):
    _classname = _prefix + "GlobalTriangleCount"


class LocalClusteringCoefficient(ScalaClassProxy):
    _classname = _prefix + "LocalClusteringCoefficient"


class LocalTriangleCount(ScalaClassProxy):
    _classname = _prefix + "LocalTriangleCount"


class SquareCount(ScalaClassProxy):
    _classname = _prefix + "SquareCount"


class ThreeNodeMotifs(ScalaClassProxy):
    _classname = _prefix + "ThreeNodeMotifs"
