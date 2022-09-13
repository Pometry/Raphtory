package com.raphtory.examples.lotr.analysis

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective

case class ArbitraryMessage()

object MinimalTestAlgorithm extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step(vertex => vertex.messageAllNeighbours(ArbitraryMessage()))
}
