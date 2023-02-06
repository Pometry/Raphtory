"""Algorithms for community detection"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.temporal.community."


class MultilayerLPA(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "MultilayerLPA"



