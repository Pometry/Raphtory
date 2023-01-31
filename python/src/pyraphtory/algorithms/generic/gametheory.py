"""Algorithms for gametheory on graphs"""


from pyraphtory.interop import ScalaClassProxy


_prefix = "com.raphtory.algorithms.generic.gametheory."


class PrisonersDilemma(ScalaClassProxy):
    _classname = _prefix + "PrisonersDilemma"
