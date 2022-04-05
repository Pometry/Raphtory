package com.raphtory.algorithms.temporal.views

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.Vertex

class MultilayerView(
    interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]
) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.multilayerView(interlayerEdgeBuilder)

}

object MultilayerView {

  def apply(
      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge] = _ => Seq.empty[InterlayerEdge]
  ) = new MultilayerView(interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge])
}
