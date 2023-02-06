"""Generic algorithms that work for both multilayer and reduced views of the graph"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy
from pyraphtory.interop import ScalaClassProxyWithImplicits

_prefix = "com.raphtory.algorithms.generic."


class AdjPlus(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "AdjPlus"


class BinaryDiffusion(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "BinaryDiffusion"


class CBOD(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "CBOD"


class ConnectedComponents(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "ConnectedComponents"


class Coreness(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "Coreness"


class EdgeList(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "EdgeList"


class GraphState(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "GraphState"


class HITS(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "HITS"


class KCore(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "KCore"


class LargestConnectedComponent(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "LargestConnectedComponent"


class MaxFlow(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "MaxFlow"


class NeighbourNames(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "NeighbourNames"


class NodeEdgeCount(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "NodeEdgeCount"


class NodeInformation(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "NodeInformation"


class NodeList(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "NodeList"


class TaintTrackingWithHistory(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "TaintTrackingWithHistory"


class TwoHopPaths(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "TwoHopPaths"


class VertexHistogram(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "VertexHistogram"