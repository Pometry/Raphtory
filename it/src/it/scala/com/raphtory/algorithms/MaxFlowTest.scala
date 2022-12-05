package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.MaxFlow
import com.raphtory.api.input.Source
import com.raphtory.spouts.ResourceSpout

class MaxFlowTest extends BaseCorrectnessTest {
  test("test on line graph") {
    correctnessTest(
            TestQuery(MaxFlow[Long]("1", "3"), 2),
            Source(ResourceSpout("MaxFlow/minimalTest.csv"), WeightedGraphBuilder),
            "MaxFlow/minimalResult.csv"
    )
  }

  test("test on two connected cliques".only) {
    correctnessTest(
            TestQuery(MaxFlow[Long]("2", "101", maxIterations = 10000), 9899),
            "MaxFlow/bottleneckResult.csv"
    )
  }

  override def setSource(): Source = Source(ResourceSpout("MaxFlow/bottleneck.csv"), WeightedGraphBuilder)
}
