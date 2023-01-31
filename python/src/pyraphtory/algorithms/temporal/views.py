"""Multilayer views of the graph"""


from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal.views"

class MultilayerView(ScalaClassProxy):
    _classname = _prefix + "MultilayerView"
