package com.raphtory.algorithms.generic.motif

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective

/**
  * {s}`LocalClusteringCoefficient()`
  *  : Returns the local clustering coefficient for each vertex.
  *
  * The clustering coefficient is the number of triangles of that vertex as
  *    a proportion of the total possible triangles of that vertex
  *
  * ## States
  *
  *  {s}`clustering: Double`
  *    : local clustering coefficient
  *
  * ## Returns
  *
  *  | vertex name       | clustering               |
  *  | ----------------- | ------------------------ |
  *  | {s}`name: String` | {s}`clustering: Double`  |
  *
  * ```{note}
  *  Edges here are treated as undirected, so if the underlying network is directed here,
  * 'neighbours' refers to the union of in-neighbours and out-neighbours.
  * ``
  */

object LocalClusteringCoefficient extends NodeList(Seq("clustering")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    LocalTriangleCount(graph)
      .step { vertex =>
        val tC = vertex.getState[Int]("triangles")
        val k  = vertex.degree
        vertex.setState("clustering", if (k > 1) 2.0 * tC.toDouble / (k * (k - 1)) else 0.0)
      }

}
