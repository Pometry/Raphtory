package com.raphtory.algorithms.api

import com.raphtory.graph.visitor.Vertex

class VertexFilter(f: Vertex => Boolean) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.step(vertex => if (!f(vertex)) vertex.remove())

}

object VertexFilter {
  def apply(f: Vertex => Boolean) = new VertexFilter(f)
}
