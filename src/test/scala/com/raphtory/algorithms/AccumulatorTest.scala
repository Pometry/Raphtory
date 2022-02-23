package com.raphtory.algorithms

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.algorithm.GraphState
import com.raphtory.core.algorithm.Row
import com.raphtory.core.algorithm.Table
import com.raphtory.core.graph.visitor.Vertex

class CountNodes extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .setGlobalState { globalState: GraphState =>
        globalState.newAdder[Int]("nodeCount")
      }
      .step { (vertex: Vertex, globalState: GraphState) =>
        globalState("nodeCount") += 1
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .globalSelect(graphState => Row(graphState("nodeCount").lastValue))

}

object CountNodes {
  def apply() = new CountNodes
}

class AccumulatorTest extends BaseCorrectnessTest {
  test("Test accumulators by counting nodes") {
    assert(correctnessTest(CountNodes(), "MotifCount/motiftest.csv", "Accumulator/results.csv", 23))
  }
}
