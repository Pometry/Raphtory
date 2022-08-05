from pyraphtory.proxy import ScalaClassProxy


class Int(ScalaClassProxy):
    _classname = "scala.math.Numeric.LongIsIntegral"


class Double(ScalaClassProxy):
    _classname = "scala.math.Numeric.DoubleIsFractional"
