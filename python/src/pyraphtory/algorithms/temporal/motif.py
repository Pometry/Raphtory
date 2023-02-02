"""Algorithms for finding motif counts in temporal graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.temporal.motif."


class LocalThreeNodeMotifs(ScalaAlgorithm):
    _classname = _prefix + "LocalThreeNodeMotifs"


class MotifAlpha(ScalaAlgorithm):
    _classname = _prefix + "MotifAlpha"


class ThreeNodeMotifs(ScalaAlgorithm):
    _classname = _prefix + "ThreeNodeMotifs"


