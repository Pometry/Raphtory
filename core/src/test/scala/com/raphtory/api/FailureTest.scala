package com.raphtory.api

import cats.effect.IO
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.components.querymanager.GraphFunctionComplete
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.SequenceSpout
import com.raphtory.BasicGraphBuilder
import com.raphtory.Raphtory
import com.raphtory.internals.communication.EndPoint
import munit.CatsEffectSuite

class FailingAlgo extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step(_ => throw new Exception("Algorithm failed"))
}

class FailureTest extends CatsEffectSuite {
  test("test failure propagation for failure in algorithm step") {
    Raphtory
      .streamIO(
              spout = SequenceSpout("1,1,1"),
              graphBuilder = BasicGraphBuilder()
      )
      .use { graph =>
        IO {

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
        }
      }
  }
}
