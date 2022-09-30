package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.MaxFlow
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.communication.SchemaProviderInstances._

class MaxFlowTest extends BaseCorrectnessTest {
  test("test on line graph") {
    correctnessTest(
            TestQuery(MaxFlow[Long]("1", "3"), 2),
            "MaxFlow/minimalTest.csv",
            "MaxFlow/minimalResult.csv"
    )
  }
  test("test on two connected cliques") {
    correctnessTest(
            TestQuery(MaxFlow[Long]("2", "101", maxIterations = 10000), 9899),
            "MaxFlow/bottleneck.csv",
            "MaxFlow/bottleneckResult.csv"
    )
  }
}
