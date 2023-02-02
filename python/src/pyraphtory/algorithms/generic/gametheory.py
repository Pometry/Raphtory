"""Algorithms for gametheory on graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.generic.gametheory."


class PrisonersDilemma(ScalaAlgorithm, ScalaClassProxy):
    _classname = _prefix + "PrisonersDilemma"
