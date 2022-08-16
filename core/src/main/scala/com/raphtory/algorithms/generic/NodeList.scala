package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.NodeListOutput
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/**
  * {s}`NodeList(properties: Seq[String] = Seq.empty[String], defaults: Map[String, Any] = Map.empty[String, Any])`
  * {s}`NodeList(properties: String*)`
  * {s}`NodeList(defaults: Map[String, Any], properties: String*)`
  * : Write out nodes with selected properties to table with optional default values
  *
  * The first column in the table is the node's name and the other columns correspond to the selected properties
  *
  * Property names are looked up on a node's computational state first and then in a node's property.
  * This means that this algorithm can be used as the last step in an algorithm chain to include output
  * of intermediate results or non-default state. It is also useful as the base class for custom algorithms that
  * should return a table of vertex states as result.
  *
  * ## Parameters
  *
  * {s}`properties: Seq[String]`
  * : Sequence of property names
  *
  * {s}`defaults: Map[String, Any]`
  * : Map from property names to default values
  * (if a property name is not found on a node, it is first looked up in defaults,
  * and if not found there, set to {s}`None`)
  *
  * ## Returns
  *
  * | vertex name       | Property1       | ... |
  * | ----------------- | --------------- | --- |
  * | {s}`name: String` | {s}`value: Any` | ... |
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.ConnectedComponents),
  * [](com.raphtory.algorithms.generic.centrality.AverageNeighbourDegree),
  * [](com.raphtory.algorithms.generic.centrality.Degree),
  * [](com.raphtory.algorithms.generic.centrality.Distinctiveness),
  * [](com.raphtory.algorithms.generic.centrality.PageRank),
  * [](com.raphtory.algorithms.generic.centrality.WeightedDegree),
  * [](com.raphtory.algorithms.generic.centrality.WeightedPageRank),
  * [](com.raphtory.algorithms.generic.community.LPA),
  * [](com.raphtory.algorithms.generic.dynamic.DiscreteSI),
  * [](com.raphtory.algorithms.generic.dynamic.WattsCascade)
  * ```
  */
class NodeList(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends NodeListOutput(properties, defaults)
        with Generic {}

object NodeList {

  def apply(
      properties: Iterable[String] = Seq.empty[String],
      defaults: Map[String, Any] = Map.empty[String, Any]
  ) = new NodeList(properties.toSeq, defaults)

  def apply(properties: String*) = new NodeList(properties)

  def apply(defaults: Map[String, Any], properties: String*) = new NodeList(properties, defaults)
}
