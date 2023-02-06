"""Algorithms for filtering vertices and edges"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy
from pyraphtory.interop import ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.filters."

class DisparityFilter(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "DisparityFilter"


class EdgeFilter(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "EdgeFilter"


class EdgeFilterGraphState(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "EdgeFilterGraphState"


class EdgeQuantileFilter(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "EdgeQuantileFilter"


class LargestConnectedComponentFilter(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "LargestConnectedComponentFilter"


class UniformEdgeSample(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "UniformEdgeSample"


class UniformVertexSample(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "UniformVertexSample"


class VertexFilter(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "VertexFilter"


class VertexFilterGraphState(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "VertexFilterGraphState"


class VertexQuantileFilter(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "VertexQuantileFilter"


