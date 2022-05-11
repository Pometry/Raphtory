package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.GraphState
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.graph.visitor.Vertex

class VertexFilterWithGraphState(f: (Vertex, GraphState) => Boolean) extends NodeList() {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.vertexFilter(f)

}

object VertexFilterGraphState {
  def apply(f: (Vertex, GraphState) => Boolean) = new VertexFilterWithGraphState(f)
}
