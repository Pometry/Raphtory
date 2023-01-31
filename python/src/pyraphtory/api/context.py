"""
Entrypoints for local and remote Raphtory contexts.

Contexts are used to create and manage different graphs.
"""


from pyraphtory.interop import ScalaClassProxy, logger, ScalaPackage


class PyRaphtory(ScalaClassProxy):
    _classname = "com.raphtory.internals.context.PyRaphtoryContext"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        logger.debug("PyRaphtory context closed using context manager")

    algorithms = ScalaPackage("com.raphtory.algorithms")
