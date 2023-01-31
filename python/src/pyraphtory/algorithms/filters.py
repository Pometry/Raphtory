"""Algorithms for filtering vertices and edges"""

from pyraphtory.interop import ScalaClassProxy, ScalaClassProxyWithImplicits


_prefix = "com.raphtory.algorithms.filters."

class DisparityFilter(ScalaClassProxy):
    _classname = _prefix + "DisparityFilter"


class EdgeFilter(ScalaClassProxy):
    _classname = _prefix + "EdgeFilter"


class EdgeFilterGraphState(ScalaClassProxy):
    _classname = _prefix + "EdgeFilterGraphState"


class EdgeQuantileFilter(ScalaClassProxyWithImplicits):
    _classname = _prefix + "EdgeQuantileFilter"


class LargestConnectedComponentFilter(ScalaClassProxy):
    _classname = _prefix + "LargestConnectedComponentFilter"


class UniformEdgeSample(ScalaClassProxy):
    _classname = _prefix + "UniformEdgeSample"


class UniformVertexSample(ScalaClassProxy):
    _classname = _prefix + "UniformVertexSample"


class VertexFilter(ScalaClassProxy):
    _classname = _prefix + "VertexFilter"


class VertexFilterGraphState(ScalaClassProxy):
    _classname = _prefix + "VertexFilterGraphState"


class VertexQuantileFilter(ScalaClassProxyWithImplicits):
    _classname = _prefix + "VertexQuantileFilter"


