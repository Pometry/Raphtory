package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.generic.NeighbourNames
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

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
  * temporal edge list with selected properties
  *
  *  | source name          | destination name     | time stamp      |  property 1     | ... |
  *  | -------------------- | -------------------- | --------------- | --- ----------- | --- |
  *  | {s}`srcName: String` | {s}`dstName: String` | {s}`time: Long` | {s}`value: Any` | ... |
  */
class TemporalEdgeList(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends NeighbourNames {

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select { vertex =>
        val neighbourMap = vertex.getState[Map[Long, String]]("neighbourNames")
        Row(
                vertex.name(),
                vertex.getOutEdges().flatMap { edge =>
                  val dst = neighbourMap(edge.dst())
                  edge.explode().map { exEdge =>
                    dst +: exEdge.timestamp() +: properties.map(name =>
                      exEdge.getPropertyValue(name) match {
                        case Some(v) => v
                        case None    => defaults.getOrElse(name, None)
                      }
                    )
                  }
                }
        )
      }
      .explode { row =>
        row.getAs[List[Seq[String]]](1).map { neighbourProperties =>
          val rowList = row.get(0) +: neighbourProperties
          Row(rowList: _*)
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
