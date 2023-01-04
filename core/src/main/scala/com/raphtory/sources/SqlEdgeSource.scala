package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Property
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate

import java.sql.ResultSet

class SqlEdgeSource(
    conn: SqlConnection,
    query: String,
    source: String,
    target: String,
    time: String,
    edgeProperties: List[String],
    sourceProperties: List[String],
    targetProperties: List[String]
) extends SqlSource(conn, query) {
  import SqlSource._

  private val sourceCol          = source.toUpperCase
  private val targetCol          = target.toUpperCase
  private val timeCol            = time.toUpperCase
  private val edgePropertyCols   = edgeProperties.map(col => col.toUpperCase)
  private val sourcePropertyCols = sourceProperties.map(col => col.toUpperCase)
  private val targetPropertyCols = targetProperties.map(col => col.toUpperCase)

  override protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => GraphUpdate = {
    val sourceIsInteger                                   = integerTypes contains columnTypes(sourceCol)
    val targetIsInteger                                   = integerTypes contains columnTypes(targetCol)
    val timeIsInteger                                     = integerTypes contains columnTypes(timeCol)
    val edgePropertyIndexes                               = 4 until (4 + edgePropertyCols.size)
    val edgePropertyBuilders: List[ResultSet => Property] = edgePropertyCols zip edgePropertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }

    (rs: ResultSet, index: Long) => {
      val sourceId       = if (sourceIsInteger) rs.getLong(1) else Graph.assignID(rs.getString(1))
      val targetId       = if (targetIsInteger) rs.getLong(2) else Graph.assignID(rs.getString(2))
      val epoch          = if (timeIsInteger) rs.getLong(3) else rs.getTimestamp(3).getTime
      val edgeProperties = edgePropertyBuilders map (_.apply(rs))
      EdgeAdd(epoch, index, sourceId, targetId, Properties(edgeProperties: _*), None)
    }
  }

  override protected def expectedColumnTypes: Map[String, List[Int]] = {
    val mainTypes  = Map(sourceCol -> idTypes, targetCol -> idTypes, timeCol -> epochTypes)
    val properties = edgePropertyCols ++ sourcePropertyCols ++ targetPropertyCols
    val propTypes  = properties map (property => (property, propertyTypes))
    mainTypes ++ propTypes.toMap
  }

  override def expectedColumns: List[String] =
    List(sourceCol, targetCol, timeCol) ++ edgePropertyCols ++ sourcePropertyCols ++ targetPropertyCols
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
