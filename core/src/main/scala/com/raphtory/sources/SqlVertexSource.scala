package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Property
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd

import java.sql.ResultSet

class SqlVertexSource(conn: SqlConnection, query: String, id: String, time: String, properties: List[String])
        extends SqlSource(conn, query) {
  import SqlSource._

  private val idCol        = id.toUpperCase
  private val timeCol      = time.toUpperCase
  private val propertyCols = properties.map(col => col.toUpperCase)

  override protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => GraphUpdate = {
    val idIsInteger                                   = integerTypes contains columnTypes(idCol)
    val timeIsInteger                                 = integerTypes contains columnTypes(timeCol)
    // TODO: write this so we don't rely on writing '3' appropriately and also in the edge source
    val propertyIndexes                               = 3 until (3 + propertyCols.size)
    val propertyBuilders: List[ResultSet => Property] = propertyCols zip propertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }

    (rs: ResultSet, index: Long) => {
      val id         = if (idIsInteger) rs.getLong(1) else Graph.assignID(rs.getString(1))
      val epoch      = if (timeIsInteger) rs.getLong(2) else rs.getTimestamp(2).getTime
      val properties = propertyBuilders map (_.apply(rs))
      VertexAdd(epoch, index, id, Properties(properties: _*), None)
    }
  }

  override protected def expectedColumnTypes: Map[String, List[Int]] = {
    val mainTypes = Map(idCol -> idTypes, timeCol -> epochTypes)
    val propTypes = propertyCols map (property => (property, propertyTypes))
    mainTypes ++ propTypes.toMap
  }

  override protected def expectedColumns: List[String] = List(idCol, timeCol) ++ propertyCols
}

object SqlVertexSource {

  def apply(conn: SqlConnection, query: String, id: String, time: String): Source =
    new SqlVertexSource(conn, query, id, time, List())

  def apply(conn: SqlConnection, query: String, id: String, time: String, properties: List[String]): Source =
    new SqlVertexSource(conn, query, id, time, properties)
}
