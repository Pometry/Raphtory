import pyraphtory.interop
from pyraphtory import interop


class PyRaphtory(interop.ScalaClassProxy):
    _classname = "com.raphtory.Raphtory$"

    algorithms = interop.ScalaPackage("com.raphtory.algorithms")
