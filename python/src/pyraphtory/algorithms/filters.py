"""Algorithms for filtering vertices and edges"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.filters."

class DisparityFilter(ScalaAlgorithm):
    _classname = _prefix + "DisparityFilter"


class EdgeFilter(ScalaAlgorithm):
    _classname = _prefix + "EdgeFilter"


class EdgeFilterGraphState(ScalaAlgorithm):
    _classname = _prefix + "EdgeFilterGraphState"


class EdgeQuantileFilter(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "EdgeQuantileFilter"


class LargestConnectedComponentFilter(ScalaAlgorithm):
    _classname = _prefix + "LargestConnectedComponentFilter"


class UniformEdgeSample(ScalaAlgorithm):
    _classname = _prefix + "UniformEdgeSample"


class UniformVertexSample(ScalaAlgorithm):
    _classname = _prefix + "UniformVertexSample"


class VertexFilter(ScalaAlgorithm):
    _classname = _prefix + "VertexFilter"


class VertexFilterGraphState(ScalaAlgorithm):
    _classname = _prefix + "VertexFilterGraphState"


class VertexQuantileFilter(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "VertexQuantileFilter"


