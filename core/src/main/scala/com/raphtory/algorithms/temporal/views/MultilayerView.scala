package com.raphtory.algorithms.temporal.views

import com.raphtory.api.algorithm.MultilayerProjection
import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.visitor.InterlayerEdge
import com.raphtory.api.visitor.Vertex

class MultilayerView(
    interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]
) extends MultilayerProjection {

  override def apply(graph: GraphPerspective): graph.MultilayerGraph =
    graph.multilayerView(interlayerEdgeBuilder)

}

object MultilayerView {

  def apply(
      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge] = _ => Seq.empty[InterlayerEdge]
  ) = new MultilayerView(interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge])
}
