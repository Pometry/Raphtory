package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.algorithm.Row
import com.raphtory.core.algorithm.Table
import com.raphtory.core.graph.visitor.Vertex

/**
  * {s}`Distinctiveness(alpha: Double=1.0, weightProperty="weight")`
  *  : compute distinctiveness centralities of nodes
  *
  * Distinctiveness centrality measures importance of a node through how it
  * bridges different parts of the graph, approximating this by connections to
  * low degree nodes. For more info read
  * [Distinctiveness centrality in social networks](https://journals.plos.org/plosone/article/file?id=10.1371/journal.pone.0233276&type=printable)
  *
  * ```{note}
  * The network is treated as undirected.
  * ```
  *
  * ## Parameters
  *
  *  {s}`alpha: Double = 1.0`
  *    : tuning exponent
  *
  *  {s}`weightProperty: String = "weight"`
  *    : name of property to use for edge weight. If not found, edge weight is treated as number of edge occurrences.
  *
  * ## States
  *
  *  {s}`D1: Double`, ... , {s}`D5: Double`
  *    : versions of distinctiveness centrality
  *
  * ## Returns
  *
  *  | vertex name       | D1              | ... | D5              |
  *  | ----------------- | --------------- | --- | --------------- |
  *  | {s}`name: String` | {s}`D1: Double` | ... | {s}`D5: Double` |
  */
import scala.math.log10
import scala.math.pow

class Distinctiveness(alpha: Double = 1.0, weightProperty: String = "weight")
        extends NodeList(Seq("D1", "D2", "D3", "D4", "D5")) {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        val edges  = vertex.getEdges()
        val degree = edges.size

        // sum of edge weights each exponentiated by alpha, purely for D3 & D4
        val weight =
          edges.map(e => pow(e.getPropertyOrElse(weightProperty, e.history().size), alpha)).sum
        // sum of edge weights, purely for D3
        val nodeWeight =
          edges.map(e => e.getPropertyOrElse(weightProperty, e.history().size).toDouble).sum

        vertex.messageAllNeighbours(vertex.ID(), degree, weight, nodeWeight)
      }
      .step { vertex =>
        val messages = vertex.messageQueue[(Long, Int, Double, Double)]
        val N        = graph.nodeCount().toDouble
        vertex.setState("D1", D1(vertex, messages, N, alpha))
        vertex.setState("D2", D2(vertex, messages, N, alpha))
        vertex.setState("D3", D3(vertex, messages, N, alpha))
        vertex.setState("D4", D4(vertex, messages, N, alpha))
        vertex.setState("D5", D5(vertex, messages, N, alpha))
      }

  def D1(
      vertex: Vertex,
      messages: List[(Long, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (id, degree, _, _) =>
          val edge       = vertex.getEdge(id).head
          val edgeWeight = edge.getPropertyOrElse(weightProperty, edge.history().size).toDouble
          edgeWeight * (log10(noNodes - 1) - alpha * log10(degree))
      })
      .sum

  def D2(
      vertex: Vertex,
      messages: List[(Long, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (_, degree, _, _) =>
          (log10(noNodes - 1) - alpha * log10(degree))
      })
      .sum

  def D3(
      vertex: Vertex,
      messages: List[(Long, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (id, _, nodePowerWeight, nodeSumWeight) =>
          val edge       = vertex.getEdge(id).head
          val edgeWeight = edge.getPropertyOrElse(weightProperty, edge.history().size).toDouble
          edgeWeight * (log10(nodeSumWeight / 2.0) - log10(
                  nodePowerWeight - pow(edgeWeight, alpha) + 1
          ))
      })
      .sum

  def D4(
      vertex: Vertex,
      messages: List[(Long, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (id, _, nodePowerWeight, _) =>
          val edge       = vertex.getEdge(id).head
          val edgeWeight = edge.getPropertyOrElse(weightProperty, edge.history().size)
          pow(edgeWeight, alpha + 1) / nodePowerWeight
      })
      .sum

  def D5(
      vertex: Vertex,
      messages: List[(Long, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (_, degree, _, _) =>
          pow(degree, -1 * alpha)
      })
      .sum

}

object Distinctiveness {

  def apply(alpha: Double = 1.0, weightProperty: String = "weight") =
    new Distinctiveness(alpha, weightProperty)
}
