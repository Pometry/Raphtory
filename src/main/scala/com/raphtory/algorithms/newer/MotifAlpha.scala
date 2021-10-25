package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

/*

This is an implementation of the alpha_1 motif from the paper
Detecting Mixing Services via Mining Bitcoin Transaction Network With Hybrid Motifs
 */

class MotifAlpha(fileOutput:String="/tmp/motif_alpha") extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .select({
        vertex =>
          if (vertex.explodeInEdges().nonEmpty & vertex.explodeOutEdges().nonEmpty) {
            val out = vertex.explodeInEdges()
              .map(inEdge => vertex.explodeOutEdges()
                .filter( e => e.getTimestamp() > inEdge.getTimestamp() & e.dst() != inEdge.src() & e.dst() != inEdge.dst()).size).sum
            Row(vertex.ID(), out)
          }
          else{
            Row(vertex.ID(), 0)
          }
      }
      ).writeTo(fileOutput)
  }

}

object MotifAlpha{
  def apply(path:String) = new MotifAlpha(path)
}

