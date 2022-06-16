package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Vertex

import scala.math.log10
import scala.math.pow
import math.Numeric.Implicits._

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
class Distinctiveness[T](alpha: Double = 1.0, weightProperty: String = "weight")(implicit
    numeric: Numeric[T]
) extends NodeList(Seq("D1", "D2", "D3", "D4", "D5")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        val edges  = vertex.getAllEdges()
        val degree = edges.size

        // sum of edge weights each exponentiated by alpha, purely for D3 & D4
        val weight     =
          edges.map(e => pow(e.weight[T]().toDouble, alpha)).sum
        // sum of edge weights, purely for D3
        val nodeWeight = vertex.weightedTotalDegree[T]().toDouble

        vertex.messageAllNeighbours(vertex.ID, degree, weight, nodeWeight)
      }
      .step { (vertex, graphState) =>
        val messages = vertex.messageQueue[(vertex.IDType, Int, Double, Double)]
        val N        = graphState.nodeCount
        vertex.setState("D1", D1(vertex)(messages, N, alpha))
        vertex.setState("D2", D2(vertex)(messages, N, alpha))
        vertex.setState("D3", D3(vertex)(messages, N, alpha))
        vertex.setState("D4", D4(vertex)(messages, N, alpha))
        vertex.setState("D5", D5(vertex)(messages, N, alpha))
      }

  def D1(vertex: Vertex)(
      messages: List[(vertex.IDType, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (id, degree, _, _) =>
          val edgeWeight = vertex
            .getEdge(id)
            .map(_.weight[T]())
            .sum
            .toDouble
          edgeWeight * (log10(noNodes - 1) - alpha * log10(degree))
      })
      .sum

  def D2(vertex: Vertex)(
      messages: List[(vertex.IDType, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (_, degree, _, _) =>
          (log10(noNodes - 1) - alpha * log10(degree))
      })
      .sum

  def D3(vertex: Vertex)(
      messages: List[(vertex.IDType, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (id, _, nodePowerWeight, nodeSumWeight) =>
          val edgeWeight = vertex.getEdge(id).map(_.weight[T]()).sum.toDouble
          edgeWeight * (log10(nodeSumWeight / 2.0) - log10(
                  nodePowerWeight - pow(edgeWeight, alpha) + 1
          ))
      })
      .sum

  def D4(vertex: Vertex)(
      messages: List[(vertex.IDType, Int, Double, Double)],
      noNodes: Double,
      alpha: Double
  ): Double =
    messages
      .map({
        case (id, _, nodePowerWeight, _) =>
          val edgeWeight = vertex.getEdge(id).map(_.weight[T]()).sum.toDouble
          pow(edgeWeight, alpha + 1) / nodePowerWeight
      })
      .sum

  def D5(vertex: Vertex)(
      messages: List[(vertex.IDType, Int, Double, Double)],
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

  def apply[T: Numeric](alpha: Double = 1.0, weightProperty: String = "weight") =
    new Distinctiveness[T](alpha, weightProperty)
}
