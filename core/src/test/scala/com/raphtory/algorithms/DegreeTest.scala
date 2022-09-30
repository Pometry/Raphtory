package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.centrality.{Degree, WeightedDegree}
import com.raphtory.api.input.{GraphBuilder, Spout}
import com.raphtory.spouts.ResourceSpout

import scala.concurrent.duration.{Duration, FiniteDuration}

class DegreeTest extends BaseCorrectnessTest {
  override def setGraphBuilder(): GraphBuilder[String] = WeightedGraphBuilder()

  override def setSpout(): Spout[String] = ResourceSpout("Degree/degreeTest.csv")

  test("weighted Degree with weighted edges".only) {
    correctnessTest(TestQuery(WeightedDegree[Long](), 6), "Degree/weightedResult.csv")
  }

  test("weighted Degree with edge count") {
    correctnessTest(
            TestQuery(WeightedDegree[Long](), 6),
            "Degree/countedResult.csv"
    )
  }

  override def munitTimeout: Duration = FiniteDuration(15, "min")

  test("unweighted Degree") {
    correctnessTest(TestQuery(Degree(), 6), "Degree/unweightedResult.csv")
  }
}
