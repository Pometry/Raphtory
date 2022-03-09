package com.raphtory.implementations

import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.components.graphbuilder.GraphBuilder

class DegreeTest extends BaseCorrectnessTest {
  override def setGraphBuilder(): GraphBuilder[String] = WeightedGraphBuilder()

  test("weighted Degree with weighted edges") {
    assert(
            correctnessTest(
                    WeightedDegree[Long](),
                    "Degree/degreeTest.csv",
                    "Degree/weightedResult.csv",
                    6
            )
    )
  }

  test("weighted Degree with edge count") {
    assert(
            correctnessTest(
                    WeightedDegree[Long](""),
                    "Degree/degreeTest.csv",
                    "Degree/countedResult.csv",
                    6
            )
    )
  }

  test("unweighted Degree") {
    assert(correctnessTest(Degree(), "Degree/degreeTest.csv", "Degree/unweightedResult.csv", 6))
  }
}
