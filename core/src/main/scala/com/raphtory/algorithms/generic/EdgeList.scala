package com.raphtory.algorithms.generic

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

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
) extends NeighbourNames {

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .explodeSelect { vertex =>
        val neighbourMap = vertex.getState[Map[Long, String]]("neighbourNames")
        val name         = vertex.name()
        vertex
          .getOutEdges()
          .map { edge =>
            val row = name +:
              neighbourMap(edge.dst()) +: // get name of neighbour
              properties // get property values
                .map(key =>
                  edge
                    .getPropertyOrElse(key, defaults.getOrElse(key, None))
                )
            Row(row: _*)
          }
      }
}

object EdgeList {
  def apply()                                                = new EdgeList()
//  def apply(
//      properties: Seq[String] = Seq.empty[String],
//      defaults: Map[String, Any] = Map.empty[String, Any]
//  )                                                          = new EdgeList(properties, defaults)
//  def apply(properties: String*)                             = new EdgeList(properties)
//  def apply(defaults: Map[String, Any], properties: String*) = new EdgeList(properties, defaults)
}
