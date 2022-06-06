package com.raphtory.generic

import com.raphtory.BaseCorrectnessTest
import com.raphtory.BasicGraphBuilder
import com.raphtory.Raphtory
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.SequenceSpout
import org.scalatest.funsuite.AnyFunSuite

class FailingAlgo extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step(_ => throw new Exception("Algorithm failed"))
}

class FailureTest extends AnyFunSuite {
  test("test failure propagation") {
    val graph = Raphtory
      .stream(
              spout = SequenceSpout("1,1,1"),
              graphBuilder = BasicGraphBuilder()
      )

    val query = graph
      .at(1)
      .past()
      .execute(new FailingAlgo)
      .writeTo(FileSink("/tmp/raphtoryTest"), "FailingAlgo")

    for (i <- 1 to 20 if !query.isJobDone)
      Thread.sleep(1000)

    assert(
            query.isJobDone
    ) // if query failed to terminate after 20 seconds, assuming infinite loop

    graph.deployment.stop()
  }
}
