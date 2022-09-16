package test.raphtory.algorithms

import dependency.DependentMessage
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective

object TestAlgorithmWithExternalDependency extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step(vertex => vertex.messageAllNeighbours(DependentMessage))
}
