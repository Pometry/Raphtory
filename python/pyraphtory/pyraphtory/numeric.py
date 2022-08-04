from pyraphtory.proxy import GenericScalaProxy
from pyraphtory.interop import find_class


class ScalaObjectProxy(GenericScalaProxy):
    def __init__(self, jvm_object=None):
        if jvm_object is None:
            jvm_object = find_class(self._classname)
        super().__init__(jvm_object=jvm_object)


class Int(ScalaObjectProxy):
    _classname = "scala.math.Numeric.LongIsIntegral"


class Double(ScalaObjectProxy):
    _classname = "scala.math.Numeric.DoubleIsFractional"
