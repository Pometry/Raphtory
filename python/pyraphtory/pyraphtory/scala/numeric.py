"""Wrappers for providing implicit numeric value required by accumulators"""

from pyraphtory.interop import ScalaClassProxy


class Int(ScalaClassProxy):
    """Wrapper for Long implicit instance"""
    _classname = "scala.math.Numeric.LongIsIntegral"


class Double(ScalaClassProxy):
    """Wrapper for Double implicit instance"""
    _classname = "scala.math.Numeric.DoubleIsFractional"
