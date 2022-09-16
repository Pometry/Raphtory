package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective

class NeighbourhoodHeterogeneity[T](categoricalFeature:String, includeSelf:Boolean = false) extends NodeList(Seq("heterogeneity")) {
  override def apply(graph: GraphPerspective): graph.Graph = {
    graph.step{
      vertex =>
        vertex.messageAllNeighbours(vertex.getState[T](categoricalFeature))
    }
      .step{
        vertex =>
          val nbLabels = vertex.messageQueue[T]
          if (includeSelf) {
            val noDistinct = nbLabels.appended(vertex.getState[T](categoricalFeature)).distinct.size
            vertex.setState("heterogeneity",(noDistinct.toDouble-1)/(vertex.degree + 1))
          } else {
            val d = vertex.degree
            vertex.setState("heterogeneity", if (d > 0) nbLabels.distinct.size.toDouble/d else 0.0)
          }
      }
  }
}

object NeighbourhoodHeterogeneity {
  def apply[T](categoricalFeature:String, includeSelf:Boolean = false) = new NeighbourhoodHeterogeneity[T](categoricalFeature,includeSelf)
}
