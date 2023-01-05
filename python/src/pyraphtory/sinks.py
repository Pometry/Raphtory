from pyraphtory.interop import ScalaClassProxy


class Sink:
    """Base class for Raphtory sinks"""
    pass


class FileSink(ScalaClassProxy, Sink):
    _classname = "com.raphtory.sinks.FileSink"


class PrintSink(ScalaClassProxy, Sink):
    _classname = "com.raphtory.sinks.PrintSink"


# class PulsarSink(ScalaClassProxy, Sink):
#     _classname = "com.raphtory.pulsar.sink.PulsarSink"
