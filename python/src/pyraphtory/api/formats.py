from pyraphtory.interop import ScalaClassProxy, GenericScalaProxy


class Format(GenericScalaProxy):
    _classname = "com.raphtory.api.output.format.Format"


class JsonFormat(ScalaClassProxy, Format):
    _classname = "com.raphtory.formats.JsonFormat"


class CsvFormat(ScalaClassProxy, Format):
    _classname = "com.raphtory.formats.CsvFormat"
