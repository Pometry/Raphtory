package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.time.DateTimeParser

class SqlEdgeSource(
    conn: SqlConnection,
    query: String,
    source: String,
    target: String,
    time: String,
    edgeProperties: List[String],
    sourceProperties: List[String],
    targetProperties: List[String]
) extends SqlSource(conn, query, 3 + edgeProperties.size + sourceProperties.size + targetProperties.size) {
  import SqlSource._

  private val sourceCol          = source.toUpperCase
  private val targetCol          = target.toUpperCase
  private val timeCol            = time.toUpperCase
  private val edgePropertyCols   = edgeProperties.map(col => col.toUpperCase)
  private val sourcePropertyCols = sourceProperties.map(col => col.toUpperCase)
  private val targetPropertyCols = targetProperties.map(col => col.toUpperCase)

  override protected def parseTuples[F[_]](
      tuples: fs2.Stream[F, List[String]],
      columns: List[Column]
  ): fs2.Stream[F, GraphUpdate] = {
    val columnTypes     = columns.map(col => (col.column_name, col.data_type)).toMap
    val sourceIsInteger = integerTypes contains columnTypes(sourceCol)
    val targetIsInteger = integerTypes contains columnTypes(targetCol)
    val timeIsInteger   = integerTypes contains columnTypes(timeCol)
    lazy val parser     = DateTimeParser("yyyy-MM-dd HH:mm:ss[.SSS]") // TODO: review this
    tuples.zipWithIndex map {
      case (tuple, index) =>
        val sourceId = if (sourceIsInteger) tuple(0).toLong else Graph.assignID(tuple(0))
        val targetId = if (targetIsInteger) tuple(1).toLong else Graph.assignID(tuple(1))
        val epoch    = if (timeIsInteger) tuple(2).toLong else parser.parse(tuple(2))
        EdgeAdd(epoch, index, sourceId, targetId, Properties(), None)
    }
  }

  override protected def expectedColumnTypes: Map[String, List[String]] = {
    val mainTypes     = Map(sourceCol -> idTypes, targetCol -> idTypes, timeCol -> epochTypes)
    val properties    = edgePropertyCols ++ sourcePropertyCols ++ targetPropertyCols
    val propertyTypes = properties map (property => (property, allTypes))
    mainTypes ++ propertyTypes.toMap
  }

  override def buildSelectQuery(viewName: String): String = {
    val eProps = if (edgePropertyCols.nonEmpty) "," + edgePropertyCols.mkString(",") else ""
    val sProps = if (sourcePropertyCols.nonEmpty) "," + sourcePropertyCols.mkString(",") else ""
    val tProps = if (targetPropertyCols.nonEmpty) "," + targetPropertyCols.mkString(",") else ""
    s"select $sourceCol,$targetCol,$timeCol$eProps$sProps$tProps from $viewName"
  }
}

object SqlEdgeSource {

  def apply(conn: SqlConnection, query: String, source: String, target: String, time: String): Source =
    new SqlEdgeSource(conn, query, source, target, time, List(), List(), List())

  def apply(
      conn: SqlConnection,
      query: String,
      source: String,
      target: String,
      time: String,
      properties: List[String]
  ): Source = new SqlEdgeSource(conn, query, source, target, time, properties, List(), List())

  def apply(
      conn: SqlConnection,
      query: String,
      source: String,
      target: String,
      time: String,
      edgeProperties: List[String] = List(),
      sourceProperties: List[String] = List(),
      targetProperties: List[String] = List()
  ): Source = new SqlEdgeSource(conn, query, source, target, time, edgeProperties, sourceProperties, targetProperties)
}
