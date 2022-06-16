package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.ResourceSpout

class DegreeTest extends BaseCorrectnessTest(startGraph = true) {
  override def setGraphBuilder(): GraphBuilder[String] = WeightedGraphBuilder()

  override def setSpout(): Spout[String] = ResourceSpout("Degree/degreeTest.csv")

  test("weighted Degree with weighted edges") {
    assert(
            correctnessTest(
                    WeightedDegree[Long](),
                    "Degree/weightedResult.csv",
                    6
            )
    )
  }

  test("weighted Degree with edge count") {
    assert(
            correctnessTest(
                    WeightedDegree[Long](""),
                    "Degree/countedResult.csv",
                    6
            )
    )
  }

  test("unweighted Degree") {
    assert(correctnessTest(Degree, "Degree/unweightedResult.csv", 6))
  }
}
