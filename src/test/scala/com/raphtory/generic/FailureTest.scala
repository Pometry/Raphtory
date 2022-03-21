package com.raphtory.generic

import com.raphtory.algorithms.BaseCorrectnessTest
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective

class FailingAlgo extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective = {
    throw new Exception("Algorithm failed")
    graph
  }
}

class FailureTest extends BaseCorrectnessTest {
  test("test failure propagation") {
    assert(correctnessTest(new FailingAlgo, "./blank.csv", "./blank.csv", 0))
  }

}
