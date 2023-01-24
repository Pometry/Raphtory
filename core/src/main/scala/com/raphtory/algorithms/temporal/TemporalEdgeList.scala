package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.generic.NeighbourNames
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table

/**
  *  {s}`TemporalEdgeList(properties: String*)`
  *  : Writes out temporal edge list with selected properties
  *
  *  {s}`EdgeList(defaults: Map[String, Any], properties: String*)`
  *  : Specify default values for missing properties
  *
  *  {s}`EdgeList(properties: Seq[String] = Seq.empty[String], defaults: Map[String, Any] = Map.empty[String, Any])`
  *  : Specify sequence of property names
  *
  *  ## Parameters
  *
  *  {s}`properties: Seq[String]`
  *  : Sequence of property names to extract (default: empty)
  *
  *  {s}`defaults: Map[String, Any]`
  *  : Map of property names to default values (default value: None)
  *
  *  ## Returns
  *
  *  temporal edge list with selected properties
  *
  *  | source name          | destination name     | time stamp      |  property 1     | ... |
  *  | -------------------- | -------------------- | --------------- | --------------- | --- |
  *  | {s}`srcName: String` | {s}`dstName: String` | {s}`time: Long` | {s}`value: Any` | ... |
  */
class TemporalEdgeList(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    NeighbourNames(graph.reducedView)
      .step { vertex =>
        val neighbourMap                           = vertex.getState[Map[Long, String]]("neighbourNames")
        vertex.setState("name", vertex.name())
        val (neighbours, timestamps, propertyRows) = vertex
          .explodeOutEdges()
          .map { edge =>
            val propertyValues = properties.map(name => edge.getPropertyOrElse(name, defaults.getOrElse(name, None)))
            (neighbourMap(edge.dst), edge.timestamp, propertyValues)
          }
          .unzip3
        vertex.setState("neighbourName", neighbours)
        vertex.setState("neighbourTimestamp", timestamps)
        properties zip propertyRows.transpose foreach {
          case (columnName, propertyColumn) => vertex.setState(columnName, propertyColumn)
        }
      }
      .select("name" +: "neighbourName" +: "neighbourTimestamp" +: properties: _*)
      .explode("neighbourName" +: "neighbourTimestamp" +: properties: _*)
}

object TemporalEdgeList {

  def apply(
      properties: Seq[String] = Seq.empty[String],
      defaults: Map[String, Any] = Map.empty[String, Any]
  )                              = new TemporalEdgeList(properties, defaults)
  def apply(properties: String*) = new TemporalEdgeList(properties)

  def apply(defaults: Map[String, Any], properties: String*) =
    new TemporalEdgeList(properties, defaults)
}
