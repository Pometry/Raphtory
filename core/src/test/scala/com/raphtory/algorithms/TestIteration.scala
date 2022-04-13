package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.graph.visitor.Vertex

class CountIterations(num_iters_before_vote: Int, num_iters: Int) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
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
