"""Algorithms for finding motif counts in temporal graphs"""


from pyraphtory.interop import ScalaClassProxy


_prefix = "package com.raphtory.algorithms.temporal.motif."


class LocalThreeNodeMotifs(ScalaClassProxy):
    _classname = _prefix + "LocalThreeNodeMotifs"


class MotifAlpha(ScalaClassProxy):
    _classname = _prefix + "MotifAlpha"


class ThreeNodeMotifs(ScalaClassProxy):
    _classname = _prefix + "ThreeNodeMotifs"


