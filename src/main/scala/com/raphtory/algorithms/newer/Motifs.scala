package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class Motifs(fileOutput:String) extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .select({
        vertex =>
          if (vertex.explodeInEdges().size > 0 & vertex.explodeOutEdges().size > 0) {
            val out = vertex.explodeInEdges()
              .map(inEdge => vertex.explodeOutEdges()
                .filter( e => e.getTimestamp() >= inEdge.getTimestamp() & e.dst() != inEdge.src() & e.dst() != inEdge.dst()).size).sum
            Row(vertex.ID(), out)
          }
          else{
            Row(vertex.ID(), 0)
          }
      }
      ).writeTo("/tmp/counter")
  }

}

object Motifs{
  def apply(path:String) = new Motifs(path)
}

