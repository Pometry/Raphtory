"""Python wrappers for global GraphState api"""

from pyraphtory.interop import GenericScalaProxy, register
from pyraphtory.api.scala.implicits.bounded import Bounded
from pyraphtory.api.scala.implicits.numeric import Long, Double

@register(name="Accumulator")
class Accumulator(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.graphstate.Accumulator"

    def __iadd__(self, other):
        self._plus_eq(other)
        return self


@register(name="GraphState")
class GraphState(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.graphstate.GraphState"
    """Wrapper for the global GraphState object which enables access to graphstate using getitem syntax"""
    def __getitem__(self, key):
        return self.apply(key)

    def new_int_max(self, *args, **kwargs):
        return super().new_max[Long, Bounded.long_bounds()](*args, **kwargs)

    def new_float_max(self, *args, **kwargs):
        return super().new_max[Double, Bounded.double_bounds()](*args, **kwargs)

    def new_int_min(self, *args, **kwargs):
        return super().new_min[Long, Bounded.long_bounds()](*args, **kwargs)

    def new_float_min(self, *args, **kwargs):
        return super().new_min[Double, Bounded.double_bounds()](*args, **kwargs)
