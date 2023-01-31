"""Algorithms for dynamics on graphs"""


from pyraphtory.interop import ScalaClassProxyWithImplicits, ScalaClassProxy


_prefix = "com.raphtory.algorithms.generic.dynamic."


class DiscreteSI(ScalaClassProxy):
    _classname = _prefix + "DiscreteSI"


class Node2VecWalk(ScalaClassProxy):
    _classname = _prefix + "Node2VecWalk"


class RandomWalk(ScalaClassProxy):
    _classname = _prefix + "RandomWalk"


class WattsCascade(ScalaClassProxyWithImplicits):
    _classname = _prefix + "WattsCascade"


class WeightedRandomWalk(ScalaClassProxyWithImplicits):
    _classname = _prefix + "WeightedRandomWalk"
