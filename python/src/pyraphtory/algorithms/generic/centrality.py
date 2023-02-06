"""Algorithms for computing centralities"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy
from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.generic.centrality."


class Assortativity(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "Assortativity"


class AverageNeighbourDegree(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "AverageNeighbourDegree"


class Degree(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "Degree"


class Distinctiveness(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "Distinctiveness"


class PageRank(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "PageRank"


class WeightedDegree(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "WeightedDegree"


class WeightedPageRank(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "WeightedPageRank"