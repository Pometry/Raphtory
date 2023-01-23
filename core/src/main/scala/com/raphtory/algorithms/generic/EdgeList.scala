package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/**
  *  {s}`EdgeList(properties: String*)`
  *  : Writes out edge list with selected properties to table with format srcName, dstName, edgeProperty1, ...
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
  * edge list with selected properties
  *
  *  | source name          | destination name     | property 1      | ... |
  *  | -------------------- | -------------------- | --------------- | --- |
  *  | {s}`srcName: String` | {s}`dstName: String` | {s}`value: Any` | ... |
  */
class EdgeList(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = NeighbourNames(graph)

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .step { vertex =>
        vertex.setState("name", vertex.name())
        val neighbourMap               = vertex.getState[Map[vertex.IDType, String]]("neighbourNames")
        val (neighbours, propertyRows) = vertex.outEdges.toSeq.map { edge =>
          val propertyValues = properties.map(key => edge.getPropertyOrElse(key, defaults.getOrElse(key, None)))
          (neighbourMap(edge.dst), propertyValues)
        }.unzip
        vertex.setState("neighbourName", neighbours)
        properties zip propertyRows.transpose foreach {
          case (columnName, propertyColumn) => vertex.setState(columnName, propertyColumn)
        }
      }
      .step(vertex => println("name", vertex.name))
      .select("name" +: "neighbourName" +: properties: _*)
      .explode("neighbourName" +: properties: _*)
}

object EdgeList {
  def apply() = new EdgeList()

  def apply(
      properties: Seq[String] = Seq.empty[String],
      defaults: Map[String, Any] = Map.empty[String, Any]
  )                                                          = new EdgeList(properties, defaults)
  def apply(properties: String*)                             = new EdgeList(properties)
  def apply(defaults: Map[String, Any], properties: String*) = new EdgeList(properties, defaults)
}
