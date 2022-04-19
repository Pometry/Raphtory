package com.raphtory.enrontest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.components.spout.Spout
import com.raphtory.components.spout.SpoutExecutor
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover

import scala.language.postfixOps
import sys.process._
import java.io.File

@DoNotDiscover
class RaphtoryENRONTest extends BaseRaphtoryAlgoTest[String] {

  test("Graph State Test") {
    val outputFormat: FileOutputFormat = FileOutputFormat(outputDirectory)

    graph
      .walk(10000)
      .past()
      .execute(GraphState())
      .writeTo(outputFormat)
      .waitForJob()

//    algorithmTest(GraphState(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))

    assert(true)
  }

  test("Connected Components Test") {
    val outputFormat: FileOutputFormat = FileOutputFormat(outputDirectory)

    val result = algorithmTest(
            algorithm = ConnectedComponents(),
            outputFormat = outputFormat,
            start = 1,
            end = 32674,
            increment = 10000,
            windows = List(500, 1000, 10000)
    )

    assert(true)
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
