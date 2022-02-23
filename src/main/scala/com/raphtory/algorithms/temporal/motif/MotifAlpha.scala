package com.raphtory.algorithms.temporal.motif

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.core.algorithm.GraphPerspective

/**
  * {s}`MotifAlpha()`
  *  : count Type1 2-edge-1-node temporal motifs
  *
  * The algorithms identifies 2-edge-1-node temporal motifs; It detects one type of motifs:
  * For each incoming edge a vertex has, the algorithm checks whether there are any outgoing
  * edges which occur after it and returns a count of these.
  *
  * ````{note}
  * In other words, it detects motifs that exhibit incoming flow followed by outgoing flow in the form
  *
  * ```{image} /images/mc1.png
  * :width: 200px
  * :alt: mc type 1
  * :align: center
  * ```
  * ````
  *
  * ## States
  *
  *  {s}`motifAlpha: Int`
  *    : Number of Type-1 temporal motifs centered on the vertex
  *
  * ## Returns
  *
  *  | vertex name       | Number of Type-1 motifs |
  *  | ----------------- | ----------------------- |
  *  | {s}`name: String` | {s}`motifAlpha: Int`    |
  */
class MotifAlpha(fileOutput: String = "/tmp/motif_alpha") extends NodeList(Seq("motifAlpha")) {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.step { vertex =>
      if (vertex.explodeInEdges().nonEmpty & vertex.explodeOutEdges().nonEmpty)
        vertex.setState(
                "motifAlpha",
                vertex
                  .explodeInEdges()
                  .map(inEdge =>
                    vertex
                      .explodeOutEdges()
                      .filter(e =>
                        e.getTimestamp() > inEdge.getTimestamp() & e.dst() != inEdge.src() & e
                          .dst() != inEdge.dst()
                      )
                      .size
                  )
                  .sum
        )
      else
        vertex.setState("motifAlpha", 0)
    }
}

object MotifAlpha {
  def apply(path: String) = new MotifAlpha(path)
}
