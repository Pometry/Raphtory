package com.raphtory.generic

import com.raphtory.implementations.BaseCorrectnessTest
import com.raphtory.implementations.BasicGraphBuilder
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import org.scalatest.funsuite.AnyFunSuite

class FailingAlgo extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective = {
    throw new Exception("Algorithm failed")
    graph
  }
}

class FailureTest extends AnyFunSuite {
  test("test failure propagation") {
    val graph = Raphtory.streamGraph(graphBuilder = BasicGraphBuilder())
    val query = graph.pointQuery(new FailingAlgo, FileOutputFormat("/tmp"), 0)
    for (i <- 1 to 10 if !query.isJobDone())
      Thread.sleep(1000)
    assert(
            query.isJobDone()
    ) // if query failed to terminate after 10 seconds, assuming infinite loop
    graph.stop()
  }

}
