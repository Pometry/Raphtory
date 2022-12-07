from pyraphtory.interop import ScalaClassProxy

class Source(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Source"

class CSVEdgeListSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.CSVEdgeListSource"

class JSONEdgeListSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.JSONEdgeListSource"

class JSONSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.JSONSource"
