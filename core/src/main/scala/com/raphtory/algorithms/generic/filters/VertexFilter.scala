package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.graph.visitor.Vertex

class VertexFilter(f: Vertex => Boolean) extends NodeList() {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.vertexFilter(f)

}

object VertexFilter {
  def apply(f: Vertex => Boolean) = new VertexFilter(f)
}
