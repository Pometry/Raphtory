package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.generic.NodeList

/**
  * {s}`Degree()`
  *  : return in-degree, out-degree, and degree of nodes
  *
  * The degree of a node in an undirected networks counts the number of neighbours that
  * node has. In directed networks, the in-degree of a note counts the number of incoming edges and the
  * out-degree the number of outgoing edges.
  *
  * ## States
  *
  *  {s}`inDegree: Int`
  *    : The in-degree of the node
  *
  *  {s}`outDegree: Int`
  *    : The out-degree of the node
  *
  *  {s}`degree: Int`
  *    : The undirected degree (i.e. the overall number of neighbours)
  *
  * ## Returns
  *
  *  | vertex name       | in-degree          | out-degree          | degree           |
  *  | ----------------- | ------------------ | ------------------- | ---------------- |
  *  | {s}`name: String` | {s}`inDegree: Int` | {s}`outDegree: Int` | {s}`degree: Int` |
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.centrality.WeightedDegree)
  * ```
  */
class Degree extends NodeList(Seq("inDegree", "outDegree", "degree")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step { vertex =>
      vertex.setState("inDegree", vertex.inDegree)
      vertex.setState("outDegree", vertex.outDegree)
      vertex.setState("degree", vertex.degree)
    }
}

object Degree {
  def apply() = new Degree
}
