package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.{Generic, GraphStateOutput}

/**
  * {s}`GraphState(properties: Seq[String] = Seq.empty[String], defaults: Map[String, Any] = Map.empty[String, Any])`
  * {s}`GraphState(properties: String*)`
  * {s}`GraphState(defaults: Map[String, Any], properties: String*)`
  * : Write out selected graph properties to table with optional default values.
  *
  * The different columns correspond to the selected properties.
  *
  * Property names are looked up on the graph state (either as general accumulators or graph constants.)
  * This means that this algorithm can be used as the last step in an algorithm chain to include output
  * of intermediate results or non-default state. It is also useful as the base class for custom algorithms that
  * should return a table of graph properties as result.
  *
  * ## Parameters
  *
  * {s}`properties: Seq[String]`
  * : Sequence of property names
  *
  * {s}`defaults: Map[String, Any]`
  * : Map from property names to default values
  * (if a property name is not found on in the graph state, it is first looked up in defaults,
  * and if not found there, set to {s}`None`)
  *
  * ## Returns
  *
  * | Property1       | Property2       | ... |
  * | --------------- | --------------- |     |
  * | {s}`value: Any` | {s}`value: Any` | ... |
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.motif.GlobalTriangleCount)
  * [](com.raphtory.algorithms.generic.LargestConnectedComponent)
  * [](com.raphtory.algorithms.generic.NodeEdgeCount)
  * ```
  */

class GraphState (properties: Seq[String] = Seq.empty[String],
                   defaults: Map[String, Any] = Map.empty[String, Any])
  extends GraphStateOutput(properties,defaults) with Generic {}

object GraphState {
  def apply(properties: Iterable[String] = Seq.empty[String],
            defaults: Map[String, Any] = Map.empty[String, Any]) = new GraphState(properties.toSeq, defaults)

  def apply(properties: String*) = new GraphState(properties)

  def apply(defaults: Map[String, Any], properties: String*) = new GraphState(properties, defaults)
  }