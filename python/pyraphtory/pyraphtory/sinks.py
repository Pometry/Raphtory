from pyraphtory.interop import ScalaClassProxy


class FileSink(ScalaClassProxy):
    _classname = "com.raphtory.sinks.FileSink"


class PrintSink(ScalaClassProxy):
    _classname = "com.raphtory.sinks.PrintSink"


class PulsarSink(ScalaClassProxy):
    _classname = "com.raphtory.pulsar.sink.PulsarSink"
