package com.raphtory.algorithms.generic.centrality

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}
import com.raphtory.core.model.graph.visitor.Vertex

/**
Description
  Distinctiveness centrality measures importance of a node through how it
  bridges different parts of the graph, approximating this by connections to
  low degree nodes. For more info read ref https://journals.plos.org/plosone/article/file?id=10.1371/journal.pone.0233276&type=printable

Parameters
  path String : output folder path location
  alpha Double : tuning exponent, default value is 1.0

Returns
  ID (Long) : This is the ID of the vertex
  D1 - D5 (Double) : centrality value according to each version of distinctiveness centrality

Notes
  Edges here are treated as undirected, weight treated as number of edge occurrences if not supplied

  **/

import scala.math.{log10, pow}

class Distinctiveness(path:String, alpha:Double=1.0) extends GraphAlgorithm{

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step({
      vertex =>
        val edges = vertex.getEdges()
        val degree = edges.size

        // sum of edge weights each exponentiated by alpha, purely for D3 & D4
        val weight = edges.map(e => pow(e.getPropertyOrElse("weight", e.history().size), alpha)).sum
        // sum of edge weights, purely for D3
        val nodeWeight = edges.map(e => e.getPropertyOrElse("weight", e.history().size).toDouble).sum

        vertex.messageAllNeighbours(vertex.ID(), degree, weight, nodeWeight)
    })
  }

  override def tabularise(graph: GraphPerspective): Table = {
    val N = graph.nodeCount().toDouble
    graph.select({
      vertex =>
        val messages = vertex.messageQueue[(Long, Int, Double, Double)]
        val D1c = D1(vertex, messages, N, alpha)
        val D2c = D2(vertex, messages, N, alpha)
        val D3c = D3(vertex, messages, N, alpha)
        val D4c = D4(vertex, messages, N, alpha)
        val D5c = D5(vertex, messages, N, alpha)
        Row(vertex.getPropertyOrElse("name", vertex.ID()), D1c, D2c, D3c, D4c, D5c)
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(path)
  }

  def D1(vertex: Vertex, messages:List[(Long, Int, Double, Double)], noNodes:Double, alpha:Double): Double = {
    messages.map({
      case(id, degree, _, _) =>
        val edge = vertex.getEdge(id).head
        val edgeWeight = edge.getPropertyOrElse("weight",edge.history().size).toDouble
        edgeWeight*(log10(noNodes-1) - alpha*log10(degree))
    }).sum
  }

  def D2(vertex: Vertex, messages:List[(Long, Int, Double, Double)], noNodes:Double, alpha:Double): Double = {
    messages.map({
      case(_, degree, _, _) =>
        (log10(noNodes-1) - alpha*log10(degree))
    }).sum
  }

  def D3(vertex: Vertex, messages:List[(Long, Int, Double, Double)], noNodes:Double, alpha:Double): Double = {
    messages.map({
      case(id, _, nodePowerWeight, nodeSumWeight) =>
        val edge = vertex.getEdge(id).head
        val edgeWeight = edge.getPropertyOrElse("weight",edge.history().size).toDouble
        edgeWeight * (log10(nodeSumWeight/2.0) - log10(nodePowerWeight - pow(edgeWeight,alpha)+1))
    }).sum
  }

  def D4(vertex: Vertex, messages:List[(Long, Int, Double, Double)], noNodes:Double, alpha:Double): Double = {
    messages.map({
      case(id, _, nodePowerWeight, _) =>
        val edge = vertex.getEdge(id).head
        val edgeWeight = edge.getPropertyOrElse("weight",edge.history().size)
        pow(edgeWeight,alpha + 1)/nodePowerWeight
    }).sum
  }

  def D5(vertex: Vertex, messages:List[(Long, Int, Double, Double)], noNodes:Double, alpha:Double): Double = {
    messages.map({
      case(_, degree, _, _)  =>
        pow(degree,-1*alpha)
    }).sum
  }

}

object Distinctiveness {
  def apply(path:String, alpha:Double=1.0) = new Distinctiveness(path,alpha)
}
