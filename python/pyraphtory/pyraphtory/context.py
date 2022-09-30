from pyraphtory import interop, algorithm


class PyRaphtory(interop.ScalaClassProxy):
    _classname = "com.raphtory.Raphtory$"

    algorithms = algorithm.BuiltinAlgorithm("com.raphtory.algorithms")
