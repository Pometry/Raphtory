package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective}

class Motifs extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .select({
        vertex =>
          if (vertex.explodeInEdges().size > 0 & vertex.explodeOutEdges().size > 0) {
            val outEdges = vertex.explodeOutEdges()
            val inEdges = vertex.explodeInEdges()
              .foreach(
                inEdge => vertex.explodeOutEdges().filter(e => e.aliveAt(inEdge.ban))
              Row("")
            )
          }
      }
      ).writeTo("/tmp/taint_output")

  }

}
