package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.centrality.{Degree, WeightedDegree}
import com.raphtory.api.input.Source
import com.raphtory.spouts.ResourceOrFileSpout
import com.test.raphtory.WeightedGraphBuilder

class DegreeTest extends BaseCorrectnessTest {

  test("weighted Degree with weighted edges") {
    correctnessTest(TestQuery(WeightedDegree[Long](), 6), "Degree/weightedResult.csv")
  }

  test("weighted Degree with edge count") {
    correctnessTest(
            TestQuery(WeightedDegree[Long](""), 6),
            "Degree/countedResult.csv"
    )
  }

  test("unweighted Degree") {
    correctnessTest(TestQuery(Degree(), 6), "Degree/unweightedResult.csv")
  }

  override def setSource(): Source = Source(ResourceOrFileSpout("Degree/degreeTest.csv"), WeightedGraphBuilder())
}
