"""Algorithms for dynamics on graphs"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy
from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.dynamic."


class DiscreteSI(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "DiscreteSI"


class Node2VecWalk(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "Node2VecWalk"


class RandomWalk(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "RandomWalk"


class WattsCascade(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "WattsCascade"


class WeightedRandomWalk(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "WeightedRandomWalk"
