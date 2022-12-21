package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.time.DateTimeParser

class SqlVertexSource(conn: SqlConnection, query: String, id: String, time: String, properties: List[String])
        extends SqlSource(conn, query, 2 + properties.size) {
  import SqlSource._

  private val idCol        = id.toUpperCase
  private val timeCol      = time.toUpperCase
  private val propertyCols = properties.map(col => col.toUpperCase)

  override protected def parseTuples[F[_]](
      tuples: fs2.Stream[F, List[String]],
      columns: List[SqlSource.Column]
  ): fs2.Stream[F, GraphAlteration.GraphUpdate] = {
    val columnTypes   = columns.map(col => (col.column_name, col.data_type)).toMap
    val idIsInteger   = integerTypes contains columnTypes(idCol)
    val timeIsInteger = integerTypes contains columnTypes(timeCol)
    lazy val parser   = DateTimeParser("yyyy-MM-dd HH:mm:ss[.SSS]") // TODO: review this
    tuples.zipWithIndex map {
      case (tuple, index) =>
        val id    = if (idIsInteger) tuple(0).toLong else Graph.assignID(tuple(0))
        val epoch = if (timeIsInteger) tuple(1).toLong else parser.parse(tuple(1))
        VertexAdd(epoch, index, id, Properties(), None)
    }
  }

  override protected def buildSelectQuery(viewName: String): String = {
    val props = if (propertyCols.nonEmpty) "," + propertyCols.mkString(",") else ""
    s"select $idCol,$timeCol$props from $viewName"
  }

  override protected def expectedColumnTypes: Map[String, List[String]] = {
    val mainTypes     = Map(idCol -> idTypes, timeCol -> epochTypes)
    val propertyTypes = propertyCols map (property => (property, allTypes))
    mainTypes ++ propertyTypes.toMap
  }
}

object SqlVertexSource {

  def apply(conn: SqlConnection, query: String, id: String, time: String): Source =
    new SqlVertexSource(conn, query, id, time, List())

  def apply(conn: SqlConnection, query: String, id: String, time: String, properties: List[String]): Source =
    new SqlVertexSource(conn, query, id, time, properties)
}
