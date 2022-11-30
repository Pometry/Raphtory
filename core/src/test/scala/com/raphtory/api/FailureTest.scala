package com.raphtory.api

import cats.effect.IO
import cats.effect.Resource
import cats.effect.SyncIO
import com.raphtory._
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.SequenceSpout
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sources.CSVEdgeListSource
import munit.CatsEffectSuite

class FailingAlgo extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.step(_ => throw new Exception("Algorithm failed"))
}

class FailureTest extends CatsEffectSuite {

  val f: SyncIO[FunFixture[DeployedTemporalGraph]] = ResourceFixture(
          new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
            .newIOGraph(failOnNotFound = false, destroy = true)
  )

  f.test("test failure propagation for failure in algorithm step") { graph =>
    graph.load(CSVEdgeListSource(SequenceSpout("1,1,1")))
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
