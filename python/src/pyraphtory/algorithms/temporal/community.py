"""Algorithms for community detection"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.temporal.community."


class MultilayerLPA(ScalaAlgorithm):
    _classname = _prefix + "MultilayerLPA"



