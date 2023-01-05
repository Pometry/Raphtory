package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.MaxFlow
import com.raphtory.api.input.Source
import com.raphtory.spouts.ResourceOrFileSpout

class MaxFlowTest extends BaseCorrectnessTest {
  test("test on line graph") {
    correctnessTest(
            TestQuery(MaxFlow[Long]("1", "3"), 2),
            Source(ResourceOrFileSpout("/MaxFlow/minimalTest.csv"), WeightedGraphBuilder()),
            "MaxFlow/minimalResult.csv"
    )
  }

  test("test on two connected cliques") {
    correctnessTest(
            TestQuery(MaxFlow[Long]("2", "101", maxIterations = 10000), 9899),
            "MaxFlow/bottleneckResult.csv"
    )
  }

  override def setSource(): Source = Source(ResourceOrFileSpout("/MaxFlow/bottleneck.csv"), WeightedGraphBuilder())
}
