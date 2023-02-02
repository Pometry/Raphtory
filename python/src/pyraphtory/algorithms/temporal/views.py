"""Multilayer views of the graph"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal.views."


class MultilayerView(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "MultilayerView"
