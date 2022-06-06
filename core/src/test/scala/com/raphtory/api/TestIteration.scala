package com.raphtory.api

import com.raphtory.BaseCorrectnessTest
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex

class CountIterations(num_iters_before_vote: Int, num_iters: Int) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState { graphState =>
        graphState.newMax[Int]("maxIterations")
      }
      .iterate(
              { vertex: Vertex =>
                val iterations = vertex.getStateOrElse("iterations", 0) + 1
                vertex.setState("iterations", iterations)
                if (iterations >= num_iters_before_vote)
                  vertex.voteToHalt()
              },
              num_iters,
              false
      )
      .step { (vertex, graphState) =>
        graphState("maxIterations") += vertex.getState("iterations")
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(graphState => Row(graphState("maxIterations").value))
}

object CountIterations {

  def apply(num_iters_before_vote: Int = 10, num_iters: Int = 100) =
    new CountIterations(num_iters_before_vote, num_iters)
}

class TestIteration extends BaseCorrectnessTest {
  test("Testing vote-to-halt works") {
    assert(
            correctnessTest(
                    CountIterations(10, 100),
                    "MotifCount/motiftest.csv",
                    "Iterations/results.csv",
                    23
            )
    )
  }
}
