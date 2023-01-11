package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Property
import com.raphtory.api.input.Type
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd

import java.sql.ResultSet

case class SqlVertexSource(
    conn: SqlConnection,
    query: String,
    id: String,
    time: String,
    vertexType: String = "",
    properties: List[String] = List()
) extends SqlSource(conn, query) {
  import SqlSource._

  private val typeCol = if (vertexType.nonEmpty) Some(vertexType.toUpperCase) else None

  override protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => GraphUpdate = {
    val idIsInteger                                   = checkType(columnTypes, id, integerTypes)
    val timeIsInteger                                 = checkType(columnTypes, time, integerTypes)
    // TODO: check type and property columns types
    val propertiesStart                               = if (typeCol.isDefined) 4 else 3
    val propertiesEnd                                 = propertiesStart + properties.size
    val propertyIndexes                               = propertiesStart until propertiesEnd
    val propertyBuilders: List[ResultSet => Property] = properties zip propertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }

    (rs: ResultSet, index: Long) => {
      val id                     = if (idIsInteger) rs.getLong(1) else Graph.assignID(rs.getString(1))
      val epoch                  = if (timeIsInteger) rs.getLong(2) else rs.getTimestamp(2).getTime
      val vertexType             = typeCol.map(_ => Type(rs.getString(3)))
      val properties             = propertyBuilders map (_.apply(rs))
      val name: Option[Property] = if (idIsInteger) None else Some(ImmutableString("name", rs.getString(1)))
      // We add the id as the property 'name' if it is a String
      VertexAdd(epoch, index, id, Properties(properties ++ name: _*), vertexType)
    }
  }

  override protected def expectedColumnTypes: Map[String, List[Int]] = {
    val mainTypes = Map(id -> idTypes, time -> epochTypes)
    val typeTypes = typeCol.map(col => col -> stringTypes)
    val propTypes = properties map (property => (property, propertyTypes))
    mainTypes ++ typeTypes ++ propTypes.toMap
  }

  override protected def expectedColumns: List[String] =
    List(id, time) ++ typeCol ++ properties
}
