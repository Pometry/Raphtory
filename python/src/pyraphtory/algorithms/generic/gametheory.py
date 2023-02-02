"""Algorithms for gametheory on graphs"""


from pyraphtory.api.algorithm import ScalaAlgorithm


_prefix = "com.raphtory.algorithms.generic.gametheory."


class PrisonersDilemma(ScalaAlgorithm):
    _classname = _prefix + "PrisonersDilemma"
