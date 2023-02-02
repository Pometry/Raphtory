"""Generic algorithms that work for both multilayer and reduced views of the graph"""

from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxyWithImplicits

_prefix = "com.raphtory.algorithms.generic."


class AdjPlus(ScalaAlgorithm):
    _classname = _prefix + "AdjPlus"


class BinaryDiffusion(ScalaAlgorithm):
    _classname = _prefix + "BinaryDiffusion"


class CBOD(ScalaAlgorithm):
    _classname = _prefix + "CBOD"


class ConnectedComponents(ScalaAlgorithm):
    _classname = _prefix + "ConnectedComponents"


class Coreness(ScalaAlgorithm):
    _classname = _prefix + "Coreness"


class EdgeList(ScalaAlgorithm):
    _classname = _prefix + "EdgeList"


class GraphState(ScalaAlgorithm):
    _classname = _prefix + "GraphState"


class HITS(ScalaAlgorithm):
    _classname = _prefix + "HITS"


class KCore(ScalaAlgorithm):
    _classname = _prefix + "KCore"


class LargestConnectedComponent(ScalaAlgorithm):
    _classname = _prefix + "LargestConnectedComponent"


class MaxFlow(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "MaxFlow"


class NeighbourNames(ScalaAlgorithm):
    _classname = _prefix + "NeighbourNames"


class NodeEdgeCount(ScalaAlgorithm):
    _classname = _prefix + "NodeEdgeCount"


class NodeInformation(ScalaAlgorithm):
    _classname = _prefix + "NodeInformation"


class NodeList(ScalaAlgorithm):
    _classname = _prefix + "NodeList"


class TaintTrackingWithHistory(ScalaAlgorithm):
    _classname = _prefix + "TaintTrackingWithHistory"


class TwoHopPaths(ScalaAlgorithm):
    _classname = _prefix + "TwoHopPaths"


class VertexHistogram(ScalaAlgorithm, ScalaClassProxyWithImplicits):
    _classname = _prefix + "VertexHistogram"