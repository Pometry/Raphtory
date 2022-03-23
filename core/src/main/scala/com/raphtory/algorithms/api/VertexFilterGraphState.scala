package com.raphtory.algorithms.api

import com.raphtory.graph.visitor.Vertex

class VertexFilterGraphState(f: (Vertex, GraphState) => Boolean) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.step((vertex, graphState) => if (!f(vertex, graphState)) vertex.remove())

}

object VertexFilterGraphState {
  def apply(f: (Vertex, GraphState) => Boolean) = new VertexFilterGraphState(f)
}
