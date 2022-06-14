package com.raphtory.api.analysis.graphstate

import com.raphtory.BaseCorrectnessTest
import com.raphtory.BasicGraphBuilder
import com.raphtory.Raphtory
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.spouts.ResourceSpout

object CountNodes extends Generic {

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

object CountNodesTwice extends Generic {

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

object CheckNodeCount extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    CountNodes(graph)

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect { graphState =>
      val n: Int = graphState("nodeCount").value
      Row(graphState.nodeCount == n)
    }
}

class AccumulatorTest extends BaseCorrectnessTest {

  test("Test accumulators by counting nodes") {
    assert(correctnessTest(CountNodes, "MotifCount/motiftest.csv", "Accumulator/results.csv", 23))
  }
  test("Test resetting of accumulators by running CountNodes twice (should not change result)") {
    assert(
            correctnessTest(
                    CountNodes -> CountNodes,
                    "MotifCount/motiftest.csv",
                    "Accumulator/results.csv",
                    23
            )
    )
  }
  test("Test rotation of accumulators and state retention by running counting nodes twice") {
    assert(
            correctnessTest(
                    CountNodesTwice,
                    "MotifCount/motiftest.csv",
                    "Accumulator/results2.csv",
                    23
            )
    )
  }

  test("Test nodeCount on graph state is consistent for multiple perspectives") {
    graph = Raphtory.load(ResourceSpout("MotifCount/motiftest.csv"), BasicGraphBuilder())
    val job = graph
      .range(10, 23, 1)
      .window(10, Alignment.END)
      .execute(CheckNodeCount)
      .writeTo(defaultSink)

    jobId = job.getJobId
    job.waitForJob()

    getResults().foreach { res =>
      if (res.nonEmpty) {
        val t = res.split(",")
        t(t.size - 1).shouldEqual("true")
      }
    }
    graph.deployment.stop()
  }
}
