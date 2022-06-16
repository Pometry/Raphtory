package com.raphtory.api.analysis.graphstate

import cats.effect.IO
import com.raphtory.BaseCorrectnessTest
import com.raphtory.BasicGraphBuilder
import com.raphtory.Raphtory
import com.raphtory.TestQuery
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.input.Spout
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

class AccumulatorTest extends BaseCorrectnessTest(startGraph = true) {
  override def setSpout(): Spout[String] = ResourceSpout("MotifCount/motiftest.csv")

  test("Test accumulators by counting nodes") {
    correctnessTest(TestQuery(CountNodes, 23), "Accumulator/results.csv")
  }

  test("Test resetting of accumulators by running CountNodes twice (should not change result)") {
    correctnessTest(TestQuery(CountNodes -> CountNodes, 23), "Accumulator/results.csv")
  }

  test("Test rotation of accumulators and state retention by running counting nodes twice") {
    correctnessTest(TestQuery(CountNodesTwice, 23), "Accumulator/results2.csv")
  }

  test("Test nodeCount on graph state is consistent for multiple perspectives") {
    Raphtory.load(ResourceSpout("MotifCount/motiftest.csv"), BasicGraphBuilder()).use { graph =>
      IO {
        val job = graph
          .range(10, 23, 1)
          .window(10, Alignment.END)
          .execute(CheckNodeCount)
          .writeTo(defaultSink)

        val jobId = job.getJobId
        job.waitForJob()

        getResults(jobId).foreach { res =>
          if (res.nonEmpty) {
            val t = res.split(",")
            assertEquals(t(t.size - 1), "true")
          }
        }
      }
    }
  }
}
