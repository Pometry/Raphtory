package com.raphtory.algorithms

import com.raphtory.core.algorithm.{
  GraphAlgorithm,
  GraphPerspective,
  GraphState
}
import com.raphtory.core.graph.visitor.Vertex

class CountNodes extends GraphAlgorithm {
  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph
      .setGlobalState({ globalState: GraphState =>
        globalState.newAccumulator[Int]("numNodes", 0, op = _ + _)
      })
      .step((vertex: Vertex, globalState: GraphState) => {})
  }
}

class AccumulatorTest {}
