"""Python wrappers for sources (used to create graphs from files and databases)"""

from pyraphtory.interop import ScalaClassProxy

class Source(ScalaClassProxy):
    _classname = "com.raphtory.api.input.Source"

class CSVEdgeListSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.CSVEdgeListSource"

class JSONEdgeListSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.JSONEdgeListSource"

class JSONSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.JSONSource"

class SqlEdgeSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.SqlEdgeSource"

class SqlVertexSource(ScalaClassProxy):
    _classname = "com.raphtory.sources.SqlVertexSource"

class SqliteConnection(ScalaClassProxy):
    _classname = "com.raphtory.sources.SqliteConnection"

class PostgresConnection(ScalaClassProxy):
    _classname = "com.raphtory.sources.PostgresConnection"