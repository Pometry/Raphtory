package com.raphtory.generic

import com.raphtory.BaseCorrectnessTest
import com.raphtory.BasicGraphBuilder
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.SequenceSpout
import org.scalatest.funsuite.AnyFunSuite

class FailingAlgo extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.step(_ => throw new Exception("Algorithm failed"))
}

class FailureTest extends AnyFunSuite {
  test("test failure propagation") {
    val graph = Raphtory
      .streamGraph(
              spout = SequenceSpout("1,1,1"),
              graphBuilder = BasicGraphBuilder()
      )

    val query = graph.pointQuery(
            graphAlgorithm = new FailingAlgo,
            outputFormat = FileOutputFormat("/tmp/raphtoryTest"),
            timestamp = 1
    )

    for (i <- 1 to 20 if !query.isJobDone)
      Thread.sleep(1000)

    assert(
            query.isJobDone
    ) // if query failed to terminate after 20 seconds, assuming infinite loop

    graph.stop()
  }
}
