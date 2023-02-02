"""Algorithms for community detection"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.community."


class LPA(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "LPA"


class SLPA(ScalaAlgorithm):
    _classname = _prefix + "SLPA"
