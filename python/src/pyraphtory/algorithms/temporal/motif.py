"""Algorithms for finding motif counts in temporal graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal.motif."


class LocalThreeNodeMotifs(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "LocalThreeNodeMotifs"


class MotifAlpha(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "MotifAlpha"


class ThreeNodeMotifs(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "ThreeNodeMotifs"


