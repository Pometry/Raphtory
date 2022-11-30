from pyraphtory.interop import ScalaClassProxy


class JsonFormat(ScalaClassProxy):
    _classname = "com.raphtory.formats.JsonFormat"

class CsvFormat(ScalaClassProxy):
    _classname = "com.raphtory.formats.CsvFormat"    