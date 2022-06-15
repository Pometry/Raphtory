package com.raphtory.enrontest

import cats.effect.IO
import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.components.spout.SpoutExecutor
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover

import scala.language.postfixOps
import sys.process._
import java.io.File

@DoNotDiscover
class RaphtoryENRONTest extends BaseRaphtoryAlgoTest[String] {

  withGraph.test("Graph State Test") { graph =>
    val sink: FileSink = FileSink(outputDirectory)

    graph
      .walk(10000)
      .past()
      .execute(GraphState())
      .writeTo(sink)
      .waitForJob()

  }

  withGraph.test("Connected Components Test") { graph =>
    val sink: FileSink = FileSink(outputDirectory)

    algorithmTest(
            algorithm = ConnectedComponents(),
            sink = sink,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )(graph).unsafeToFuture()

  }

  override def batchLoading(): Boolean = false

  override def setSpout(): Spout[String] = FileSpout("/tmp/email_test.csv")

  override def setGraphBuilder(): GraphBuilder[String] = new ENRONGraphBuilder()

  override def setup(): Unit = {
    val path = "/tmp/email_test.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv"

    if (!new File(path).exists())
      try s"curl -o $path $url" !!
      catch {
        case ex: Exception =>
          logger.error(s"Failed to download 'email_test.csv' due to ${ex.getMessage}.")
          ex.printStackTrace()
          (s"rm $path" !)
          throw ex
      }
  }

}
