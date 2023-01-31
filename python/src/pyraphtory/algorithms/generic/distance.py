"""Algorithms for computing shortest-paths distance"""


from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.distance."


class ShortestPathDistance(ScalaClassProxyWithImplicits):
    _classname = _prefix + "ShortestPathDistance"
