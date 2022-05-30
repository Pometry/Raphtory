package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.GraphState
import com.raphtory.algorithms.api.GraphStateImplementation
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm
import com.raphtory.graph.visitor.Vertex

class CountNodes extends GenericAlgorithm {

  override def apply[G <: GraphPerspective[G]](graph: G): G =
    graph
      .setGlobalState { globalState: GraphState =>
        globalState.newAdder[Int]("nodeCount")
      }
      .step { (vertex: Vertex, globalState: GraphState) =>
        globalState("nodeCount") += 1
      }

  override def tabularise[G <: GraphPerspective[G]](graph: G): Table =
    graph
      .globalSelect(graphState => Row(graphState("nodeCount").value))

}

object CountNodes {
  def apply() = new CountNodes
}

class CountNodesTwice extends GenericAlgorithm {

  override def apply[G <: GraphPerspective[G]](graph: G): G =
    graph
      .setGlobalState { globalState: GraphState =>
        globalState.newAdder[Int]("nodeCount")
        globalState.newAdder[Int]("nodeCountDoubled", retainState = true)
      }
      .step { (vertex: Vertex, globalState: GraphState) =>
        globalState("nodeCount") += 1
        globalState("nodeCountDoubled") += 1
      }
      .step { (vertex: Vertex, globalState: GraphState) =>
        globalState("nodeCount") += 1
        globalState("nodeCountDoubled") += 1
      }

  override def tabularise[G <: GraphPerspective[G]](graph: G): Table =
    graph
      .globalSelect { graphState: GraphState =>
        Row(graphState("nodeCount").value, graphState("nodeCountDoubled").value)
      }

}

object CountNodesTwice {
  def apply() = new CountNodesTwice
}

class AccumulatorTest extends BaseCorrectnessTest {
  test("Test accumulators by counting nodes") {
    assert(correctnessTest(CountNodes(), "MotifCount/motiftest.csv", "Accumulator/results.csv", 23))
  }
  test("Test resetting of accumulators by running CountNodes twice (should not change result)") {
    assert(
            correctnessTest(
                    CountNodes() -> CountNodes(),
                    "MotifCount/motiftest.csv",
                    "Accumulator/results.csv",
                    23
            )
    )
  }
  test("Test rotation of accumulators and state retention by running counting nodes twice") {
    assert(
            correctnessTest(
                    CountNodesTwice(),
                    "MotifCount/motiftest.csv",
                    "Accumulator/results2.csv",
                    23
            )
    )
  }
}
