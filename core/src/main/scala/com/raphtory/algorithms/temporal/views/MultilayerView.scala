package com.raphtory.algorithms.temporal.views

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm
import com.raphtory.algorithms.api.algorithm.MultilayerProjectionAlgorithm
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.Vertex

class MultilayerView(
    interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge]
) extends MultilayerProjectionAlgorithm {

  override def apply[G <: GraphPerspective[G]](graph: G): graph.MultilayerGraph =
    graph.multilayerView(interlayerEdgeBuilder)

}

object MultilayerView {

  def apply(
      interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge] = _ => Seq.empty[InterlayerEdge]
  ) = new MultilayerView(interlayerEdgeBuilder: Vertex => Seq[InterlayerEdge])
}
