package com.raphtory.algorithms.temporal.views

import com.raphtory.api.analysis.algorithm.MultilayerProjection
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex

class MultilayerView(
    interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]
) extends MultilayerProjection[Any] {

  override def apply(graph: GraphPerspective): graph.MultilayerGraph =
    graph.multilayerView(interlayerEdgeBuilder)

}

object MultilayerView {

  def apply(
      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge] = _ => Seq.empty[InterlayerEdge]
  ) = new MultilayerView(interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge])
}
