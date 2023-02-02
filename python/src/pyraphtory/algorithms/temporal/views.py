"""Multilayer views of the graph"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.temporal.views."


class MultilayerView(ScalaAlgorithm):
    _classname = _prefix + "MultilayerView"
