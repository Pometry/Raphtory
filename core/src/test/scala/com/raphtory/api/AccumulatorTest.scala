package com.raphtory.api

import com.raphtory.BaseCorrectnessTest
import com.raphtory.api.algorithm.Generic
import com.raphtory.api.graphstate.GraphState
import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table
import com.raphtory.api.visitor.Vertex

class CountNodes extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState { globalState: GraphState =>
        globalState.newAdder[Int]("nodeCount")
      }
      .step { (vertex: Vertex, globalState: GraphState) =>
        globalState("nodeCount") += 1
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .globalSelect(graphState => Row(graphState("nodeCount").value))

}

object CountNodes {
  def apply() = new CountNodes
}

class CountNodesTwice extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
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

  override def tabularise(graph: GraphPerspective): Table =
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
