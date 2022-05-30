package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm
import com.raphtory.graph.visitor.ExplodedVertex

/**
  * {s}`TemporalNodeList(properties: String*) = new TemporalNodeList(properties)`
  *  : Write out timestamped nodes with selected properties to table
  *
  * {s}`TemporalNodeLisst(defaults: Map[String, Any], properties: String*)`
  *  : Specify default values for missing properties
  *
  * {s}`TemporalNodeList(properties: Seq[String] = Seq.empty[String], defaults: Map[String, Any] = Map.empty[String, Any])`
  *  : Specify property names as sequence
  *
  * Property names are looked up on a node's computational state first and then in a node's property. Property values
  * are looked up using creation timestamps explicitly. Computational state is expanded if it is a sequence
  * with the same size as the vertex's distinct creation timestamps and assuming that state maps to a vertex's
  * distinct, sorted timestamps. This means that this algorithm can be used as the last step in an algorithm
  * chain to include output of intermediate results.
  *
  * ## Params
  *
  *  {s}`properties: Seq[String]`
  *    : Sequence of property names
  *
  *  {s}`defaults: Map[String, Any]`
  *    : Map from property names to default values (if a property name is not found on a node,
  *      it is first looked up in defaults, and if not found there, set to {s}`None`)
  *
  * ## Returns
  *
  *  | vertex name       | time stamp      | property 1      | ... |
  *  | ----------------- | --------------- | --------------- | --- |
  *  | {s}`name: String` | {s}`time: Long` | {s}`value: Any` | ... |
  */
class TemporalNodeList(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends GenericAlgorithm {

  override def tabularise[G <: GraphPerspective[G]](graph: G): Table =
    graph
      .multilayerView()
      .select { vertex =>
        Row(
                vertex.baseName +: vertex.timestamp +: properties.map(key =>
                  vertex
                    .getStateOrElse(key, defaults.getOrElse(key, None), includeProperties = true)
                ): _*
        )
      }
}

object TemporalNodeList {

  def apply(
      properties: Seq[String] = Seq.empty[String],
      defaults: Map[String, Any] = Map.empty[String, Any]
  )                              = new TemporalNodeList(properties, defaults)
  def apply(properties: String*) = new TemporalNodeList(properties)

  def apply(defaults: Map[String, Any], properties: String*) =
    new TemporalNodeList(properties, defaults)
}
