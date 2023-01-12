package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Property
import com.raphtory.api.input.Type
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate

import java.sql.ResultSet

/** Source for ingesting edges from SQL Database queries
  *
  * This source ingest one edge from each of the rows returned when submitting the `query`
  * to the database referred by `conn`.
  * `source`, `target`, `time`, `edgeType`, and `properties` are used to set the names of the columns to use
  * for the ids, the timestamp, the type, and the properties for every edge.
  *
  * @param conn the connection to a specific type of database, e.g. `PostgresConnection`
  * @param query the query to be executed against the the database
  * @param source the name of the column to use as the id of the source vertex
  * @param target the name of the column to use as the id of the target vertex
  * @param time the name of the column to use as the timestamp
  * @param edgeType the name of the column to use as edge type
  * @param properties the names of the columns to use as the properties for the edge
  */
case class SqlEdgeSource(
    conn: SqlConnection,
    query: String,
    source: String,
    target: String,
    time: String,
    edgeType: String = "",
    properties: List[String] = List()
) extends SqlSource(conn, query) {
  import SqlSource._

  private val typeCol = if (edgeType.nonEmpty) Some(edgeType.toUpperCase) else None

  override protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => GraphUpdate = {
    val sourceIsInteger                               = checkType(columnTypes, source, integerTypes)
    val targetIsInteger                               = checkType(columnTypes, target, integerTypes)
    val timeIsInteger                                 = checkType(columnTypes, time, integerTypes)
    val propertiesStart                               = if (typeCol.isDefined) 5 else 4
    val propertiesEnd                                 = propertiesStart + properties.size
    val propertyIndexes                               = propertiesStart until propertiesEnd
    val propertyBuilders: List[ResultSet => Property] = properties zip propertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }

    (rs: ResultSet, index: Long) => {
      val sourceId   = if (sourceIsInteger) rs.getLong(1) else Graph.assignID(rs.getString(1))
      val targetId   = if (targetIsInteger) rs.getLong(2) else Graph.assignID(rs.getString(2))
      val epoch      = if (timeIsInteger) rs.getLong(3) else rs.getTimestamp(3).getTime
      val edgeType   = typeCol.map(_ => Type(rs.getString(4)))
      val properties = propertyBuilders map (_.apply(rs))
      EdgeAdd(epoch, index, sourceId, targetId, Properties(properties: _*), edgeType)
    }
  }

  override protected def expectedColumnTypes: Map[String, List[Int]] = {
    val mainTypes = Map(source -> idTypes, target -> idTypes, time -> epochTypes)
    val typeTypes = typeCol.map(col => col -> stringTypes)
    val propTypes = properties map (property => (property, propertyTypes))
    mainTypes ++ typeTypes ++ propTypes.toMap
  }

  override def expectedColumns: List[String] =
    List(source, target, time) ++ typeCol ++ properties
}
