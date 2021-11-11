package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}


class MotifCounting(output: String) extends GraphAlgorithm {

  def nCr(n: Int, r: Int): Int = factorial(n) / (factorial(r) * factorial(n - r))
  def factorial(n: Int): Int = {
    var res = 1
    for (i <- 2 to n) { res = res * i}
    res
  }

  // pattern 4 1 node - > two outgoing
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .select({
        vertex =>
          var pattern_a = 0 // A <= B, A <= C : one node having two out edges to two other nodes
          var pattern_b = 0 // B => A, C => A
          var pattern_c = 0 // A => B, B => C
          var pattern_d = 0 // A => B, B => C, C => B
          var pattern_e = 0 // A <= B, B => C, C => B
          if (vertex.getOutEdges().nonEmpty) {
            val unique_outgoing_edge_count = vertex.getOutEdges().map(edge => edge.dst() ).distinct.size
            pattern_a = nCr(unique_outgoing_edge_count, 2)
          }
          if (vertex.getInEdges().nonEmpty) {
            val unique_incoming_edge_count = vertex.getInEdges().map(edge => edge.src() ).distinct.size
            pattern_b = nCr(unique_incoming_edge_count, 2)
          }
          if (vertex.getInEdges().nonEmpty && vertex.getOutEdges().nonEmpty){
            val inNodes = vertex.getInEdges().map(e => e.src).toSet
            val outNodes = vertex.getOutEdges().map(e => e.dst).toSet
            pattern_c = 0
            inNodes.foreach(nid =>
              if(outNodes.contains(nid)) {
                pattern_c += outNodes.size-1
                pattern_d += inNodes.size-1

              }
              else {
                pattern_c += outNodes.size
              }
            )
          }
          Row(vertex.ID(), pattern_a, pattern_b, pattern_c, pattern_d, )
      }
      ).writeTo(output)
  }

}


object MotifCounting{
  def apply(output:String = "/tmp/LPA")  = new MotifCounting(output)
}
