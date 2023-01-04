package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
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
    properties: Seq[String] = Seq.empty[String], //if empty, get list of properties and algo state, return everything
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = NeighbourNames(graph)

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .explodeSelect { vertex =>
        val neighbourMap = vertex.getState[Map[vertex.IDType, String]]("neighbourNames")
        val name         = vertex.name()
        vertex.outEdges
          .map { edge =>
            val row =  if (properties.isEmpty) {
              val propertySet: List[String] = edge.getPropertySet()
              val stateSet: List[String] = edge.getStateSet()

              name +:
                neighbourMap(edge.dst) +: // get name of neighbour
                propertySet.map(key => edge.getPropertyOrElse(key, defaults.getOrElse(key, None))) +: stateSet.map(key => edge.getStateOrElse(key, defaults.getOrElse(key, None)))
            } else {
              name +:
                neighbourMap(edge.dst) +: // get name of neighbour
                properties
                  .map(key =>
                    edge
                      .getPropertyOrElse(key, defaults.getOrElse(key, None))
                  )
            }
            Row(row: _*)
          }
      }
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
