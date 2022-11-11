import pyraphtory.interop
from pyraphtory import interop


class PyRaphtory(interop.ScalaClassProxy):
    _classname = "com.raphtory.internals.context.PyRaphtoryContext"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        interop.logger.debug("PyRaphtory context closed using context manager")

    algorithms = interop.ScalaPackage("com.raphtory.algorithms")
