package com.raphtory.algorithms

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.MaxFlow
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.components.graphbuilder.GraphBuilder

class MaxFlowTest extends BaseCorrectnessTest {
  override def setGraphBuilder(): GraphBuilder[String] = WeightedGraphBuilder()
  test("test on line graph") {
//    assert(
//            correctnessTest(
//                    MaxFlow[Long]("1", "3"),
//                    "MaxFlow/minimalTest.csv",
//                    "MaxFlow/minimalResult.csv",
//                    2
//            )
//    )
  }
  test("test on two connected cliques") {
//    assert(
//            correctnessTest(
//                    MaxFlow[Long]("2", "101", maxIterations = 10000),
//                    "MaxFlow/bottleneck.csv",
//                    "MaxFlow/bottleneckResult.csv",
//                    9899
//            )
//    )
  }
}
