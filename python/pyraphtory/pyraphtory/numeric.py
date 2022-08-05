from pyraphtory.proxy import ScalaObjectProxy


class Int(ScalaObjectProxy):
    _classname = "scala.math.Numeric.LongIsIntegral"


class Double(ScalaObjectProxy):
    _classname = "scala.math.Numeric.DoubleIsFractional"
