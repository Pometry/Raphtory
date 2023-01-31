"""Wrappers for providing implicit numeric value required by accumulators"""

from pyraphtory.interop import ScalaClassProxy


class Long(ScalaClassProxy):
    """Wrapper for Long implicit instance"""
    _classname = "scala.math.Numeric.LongIsIntegral"


class Int(ScalaClassProxy):
    """Wrapper for Int implicit instance"""
    _classname = "scala.math.Numeric.IntIsIntegral"


class Double(ScalaClassProxy):
    """Wrapper for Double implicit instance"""
    _classname = "scala.math.Numeric.DoubleIsFractional"


class Float(ScalaClassProxy):
    """Wrapper for Float implicit instance"""
    _classname = "scala.math.Numeric.FloatIsFractional"
