"""Algorithms for computing centralities"""


from pyraphtory.interop import ScalaClassProxy, ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.centrality."


class Assortativity(ScalaClassProxy):
    _classname = _prefix + "Assortativity"


class AverageNeighbourDegree(ScalaClassProxy):
    _classname = _prefix + "AverageNeighbourDegree"


class Degree(ScalaClassProxy):
    _classname = _prefix + "Degree"


class Distinctiveness(ScalaClassProxyWithImplicits):
    _classname = _prefix + "Distinctiveness"


class PageRank(ScalaClassProxy):
    _classname = _prefix + "PageRank"


class WeightedDegree(ScalaClassProxyWithImplicits):
    _classname = _prefix + "WeightedDegree"


class WeightedPageRank(ScalaClassProxyWithImplicits):
    _classname = _prefix + "WeightedPageRank"