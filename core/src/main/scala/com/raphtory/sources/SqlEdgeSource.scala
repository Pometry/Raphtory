package com.raphtory.sources

import com.raphtory.api.input.Graph
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Property
import com.raphtory.api.input.Source
import com.raphtory.api.input.Type
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd

import java.sql.ResultSet

case class SqlEdgeSource(
    conn: SqlConnection,
    query: String,
    source: String,
    target: String,
    time: String,
    edgeType: String = "",
    sourceType: String = "",
    targetType: String = "",
    edgeProperties: List[String] = List(),
    sourceProperties: List[String] = List(),
    targetProperties: List[String] = List()
) extends SqlSource(conn, query) {
  import SqlSource._

  private val sourceCol          = source.toUpperCase
  private val targetCol          = target.toUpperCase
  private val timeCol            = time.toUpperCase
  private val edgeTypeCol        = if (edgeType.nonEmpty) Some(edgeType.toUpperCase) else None
  private val sourceTypeCol      = if (sourceType.nonEmpty) Some(sourceType.toUpperCase) else None
  private val targetTypeCol      = if (targetType.nonEmpty) Some(targetType.toUpperCase) else None
  private val typeCols           = edgeTypeCol ++ sourceTypeCol ++ targetTypeCol
  private val edgePropertyCols   = edgeProperties.map(col => col.toUpperCase)
  private val sourcePropertyCols = sourceProperties.map(col => col.toUpperCase)
  private val targetPropertyCols = targetProperties.map(col => col.toUpperCase)

  override protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => Vector[GraphUpdate] = {
    val sourceIsInteger                                   = integerTypes contains columnTypes(sourceCol)
    val targetIsInteger                                   = integerTypes contains columnTypes(targetCol)
    val timeIsInteger                                     = integerTypes contains columnTypes(timeCol)
    val sendSource                                        = sourcePropertyCols.nonEmpty
    val sendTarget                                        = targetPropertyCols.nonEmpty
    val edgePropertiesStart                               = 4 + typeCols.size
    val sourcePropertiesStart                             = edgePropertiesStart + edgePropertyCols.size
    val targetPropertiesStart                             = sourcePropertiesStart + sourcePropertyCols.size
    val targetPropertiesEnd                               = targetPropertiesStart + targetPropertyCols.size
    val edgePropertyIndexes                               = edgePropertiesStart until sourcePropertiesStart
    val sourcePropertyIndexes                             = sourcePropertiesStart until targetPropertiesStart
    val targetPropertyIndexes                             = targetPropertiesStart until targetPropertiesEnd
    val edgePropertyBuilders: List[ResultSet => Property] = edgePropertyCols zip edgePropertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }
    val sourcePropertyBuilders                            = sourcePropertyCols zip sourcePropertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }
    val targetPropertyBuilders                            = targetPropertyCols zip targetPropertyIndexes map {
      case (col, index) => getPropertyBuilder(index, col, columnTypes)
    }
    (rs: ResultSet, index: Long) => {
      val sourceId       = if (sourceIsInteger) rs.getLong(1) else Graph.assignID(rs.getString(1))
      val targetId       = if (targetIsInteger) rs.getLong(2) else Graph.assignID(rs.getString(2))
      val epoch          = if (timeIsInteger) rs.getLong(3) else rs.getTimestamp(3).getTime
      val edgeType       = typeCol.map(_ => Type(rs.getString(3)))
      val edgeProperties = edgePropertyBuilders map (_.apply(rs))
      val edge           = Vector(EdgeAdd(epoch, index, sourceId, targetId, Properties(edgeProperties: _*), edgeType))
      val source         =
        if (sendSource)
          Vector(VertexAdd(epoch, index, sourceId, Properties(sourcePropertyBuilders.map(_.apply(rs)): _*), None))
        else Vector()
      val target         =
        if (sendTarget)
          Vector(VertexAdd(epoch, index, targetId, Properties(targetPropertyBuilders.map(_.apply(rs)): _*), None))
        else Vector()

      edge ++ source ++ target // We send a VertexAdd for the source/target only if there are properties to be added
    }
  }

  override protected def expectedColumnTypes: Map[String, List[Int]] = {
    val mainTypes  = Map(sourceCol -> idTypes, targetCol -> idTypes, timeCol -> epochTypes)
    val typeTypes  = typeCols.map(col => col -> stringTypes)
    val properties = edgePropertyCols ++ sourcePropertyCols ++ targetPropertyCols
    val propTypes  = properties map (property => (property, propertyTypes))
    mainTypes ++ typeTypes ++ propTypes.toMap
  }

  override def expectedColumns: List[String] =
    List(sourceCol, targetCol, timeCol) ++ typeCols ++ edgePropertyCols ++ sourcePropertyCols ++ targetPropertyCols
}

object SqlEdgeSource {

  def apply(
      conn: SqlConnection,
      query: String,
      source: String,
      target: String,
      time: String,
      edgeType: String = "",
      sourceType: String = "",
      targetType: String = "",
      edgeProperties: List[String] = List(),
      sourceProperties: List[String] = List(),
      targetProperties: List[String] = List()
  ): Source =
    new SqlEdgeSource(
            conn,
            query,
            source,
            target,
            time,
            edgeType,
            sourceType,
            targetType,
            edgeProperties,
            sourceProperties,
            targetProperties
    )
}
