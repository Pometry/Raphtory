"""Algorithms for computing shortest-paths distance"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy
from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.distance."


class ShortestPathDistance(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "ShortestPathDistance"
