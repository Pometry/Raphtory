package com.raphtory.enrontest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.{Graph, GraphBuilder, Source, Spout}
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

import java.net.URL
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps

class RaphtoryENRONTest extends BaseRaphtoryAlgoTest[String] {

  test("Graph State Test".ignore) {

    val sink: FileSink = FileSink(outputDirectory)

    graphS
      .walk(10000)
      .past()
      .execute(GraphState())
      .writeTo(sink)
      .waitForJob()

  }

  test("Connected Components Test") {

      val sink: FileSink = FileSink(outputDirectory)

      graphS
        .range(1, 32674, 10000)
        .window(List(500, 1000, 10000))
        .execute(ConnectedComponents)
        .writeTo(sink)
        .waitForJob()

  }


  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some("/tmp/email_test.csv" -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv"))

  override def munitTimeout: Duration                      = new FiniteDuration(Int.MaxValue, TimeUnit.SECONDS)

  override def setSource(): Source = Source(FileSpout("/tmp/email_test.csv"), ENRONGraphBuilder)
}
