package com.raphtory.enrontest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

import java.net.URL
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps

class RaphtoryENRONTest extends BaseRaphtoryAlgoTest[String] {

//  withGraph.test("Graph State Test") { graph =>
//    val sink: FileSink = FileSink(outputDirectory)
//
//    graph
//      .walk(10000)
//      .past()
//      .execute(GraphState())
//      .writeTo(sink)
//      .waitForJob()
//
//  }

  test("Connected Components Test") {
    val sink: FileSink = FileSink(outputDirectory)

    algorithmTest(
            algorithm = ConnectedComponents(),
            sink = sink,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )

  }

  override def batchLoading(): Boolean = false

  override def setSpout(): Spout[String] = FileSpout("/tmp/email_test.csv")

  override def setGraphBuilder(): GraphBuilder[String] = new ENRONGraphBuilder()

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some("/tmp/email_test.csv" -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv"))

  override def munitTimeout: Duration                      = new FiniteDuration(Int.MaxValue, TimeUnit.SECONDS)

}
