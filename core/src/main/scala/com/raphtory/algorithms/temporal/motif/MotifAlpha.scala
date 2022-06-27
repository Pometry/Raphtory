package com.raphtory.algorithms.temporal.motif

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective

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
object MotifAlpha extends NodeList(Seq("motifAlpha")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step { vertex =>
      if (vertex.explodeInEdges().nonEmpty & vertex.explodeOutEdges().nonEmpty)
        vertex.setState(
                "motifAlpha",
                vertex
                  .explodeInEdges()
                  .map(inEdge =>
                    vertex
                      .explodeOutEdges()
                      .count(e => e.timestamp > inEdge.timestamp & e.dst != inEdge.src & e.dst != inEdge.dst)
                  )
                  .sum
        )
      else
        vertex.setState("motifAlpha", 0)
    }
}
