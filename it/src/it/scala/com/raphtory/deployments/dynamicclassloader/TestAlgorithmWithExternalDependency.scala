package com.raphtory.deployments.dynamicclassloader

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.communication.SchemaProviderInstances._

object TestAlgorithmWithExternalDependency extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step(vertex => vertex.messageAllNeighbours(DependentMessage))
}
