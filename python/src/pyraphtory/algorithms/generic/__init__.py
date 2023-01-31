"""Generic algorithms that work for both multilayer and reduced views of the graph"""


from pyraphtory.interop import ScalaClassProxy, ScalaClassProxyWithImplicits

_prefix = "com.raphtory.algorithms.generic."


class AdjPlus(ScalaClassProxy):
    _classname = _prefix + "AdjPlus"


class BinaryDiffusion(ScalaClassProxy):
    _classname = _prefix + "BinaryDiffusion"


class CBOD(ScalaClassProxy):
    _classname = _prefix + "CBOD"


class ConnectedComponents(ScalaClassProxy):
    _classname = _prefix + "ConnectedComponents"


class Coreness(ScalaClassProxy):
    _classname = _prefix + "Coreness"


class EdgeList(ScalaClassProxy):
    _classname = _prefix + "EdgeList"


class GraphState(ScalaClassProxy):
    _classname = _prefix + "GraphState"


class HITS(ScalaClassProxy):
    _classname = _prefix + "HITS"


class KCore(ScalaClassProxy):
    _classname = _prefix + "KCore"


class LargestConnectedComponent(ScalaClassProxy):
    _classname = _prefix + "LargestConnectedComponent"


class MaxFlow(ScalaClassProxyWithImplicits):
    _classname = _prefix + "MaxFlow"


class NeighbourNames(ScalaClassProxy):
    _classname = _prefix + "NeighbourNames"


class NodeEdgeCount(ScalaClassProxy):
    _classname = _prefix + "NodeEdgeCount"


class NodeInformation(ScalaClassProxy):
    _classname = _prefix + "NodeInformation"


class NodeList(ScalaClassProxy):
    _classname = _prefix + "NodeList"


class TaintTrackingWithHistory(ScalaClassProxy):
    _classname = _prefix + "TaintTrackingWithHistory"


class TwoHopPaths(ScalaClassProxy):
    _classname = _prefix + "TwoHopPaths"


class VertexHistogram(ScalaClassProxyWithImplicits):
    _classname = _prefix + "VertexHistogram"