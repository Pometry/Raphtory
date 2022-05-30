package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.generic.NeighbourNames
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.graph.visitor.ExplodedVertex

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
) extends GraphAlgorithm {

  override def tabularise(graph: GraphPerspective): Table =
    NeighbourNames()(graph.reducedView)
      .multilayerView()
      .explodeSelect { vertex =>
        val neighbourMap = vertex.getState[Map[Long, String]]("neighbourNames")
        vertex
          .getOutEdges()
          .map { edge =>
            Row(
                    vertex.baseName +:
                      neighbourMap(edge.dst._1) +:
                      edge.timestamp +:
                      properties.map { name =>
                        edge.getPropertyOrElse(name, defaults.getOrElse(name, None))
                      }: _*
            )
          }
      }
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
