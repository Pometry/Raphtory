package com.raphtory.algorithms.generic

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.algorithm.Row
import com.raphtory.core.algorithm.Table

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
        Row(
                name,
                vertex
                  .getOutEdges()
                  .map { edge =>
                    neighbourMap(edge.dst()) +:
                      properties
                        .map(name =>
                          edge
                            .getPropertyOrElse(name, defaults.getOrElse(name, None))
                        )
                  }
        ).getAs[List[Seq[String]]](1).map { neighbourProperties =>
          val rowList = name +: neighbourProperties
          Row(rowList: _*)
        }
      }
//      .select { vertex =>
//        val neighbourMap = vertex.getState[Map[Long, String]]("neighbourNames")
//        Row(
//                vertex.name(),
//                vertex.getOutEdges().map { edge =>
//                  neighbourMap(edge.dst()) +: properties
//                    .map(name => edge.getPropertyOrElse(name, defaults.getOrElse(name, None)))
//                }
//        )
//      }
//      .explode { row =>
//        row.getAs[List[Seq[String]]](1).map { neighbourProperties =>
//          val rowList = row.get(0) +: neighbourProperties
//          Row(rowList: _*)
//        }
//      }
}

object EdgeList {

  def apply(
      properties: Seq[String] = Seq.empty[String],
      defaults: Map[String, Any] = Map.empty[String, Any]
  )                                                          = new EdgeList(properties, defaults)
  def apply(properties: String*)                             = new EdgeList(properties)
  def apply(defaults: Map[String, Any], properties: String*) = new EdgeList(properties, defaults)
}
