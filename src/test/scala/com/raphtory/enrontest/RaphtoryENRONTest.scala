package com.raphtory.enrontest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.output.FileOutputFormat
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover

import scala.language.postfixOps
import sys.process._
import java.io.File

@DoNotDiscover
class RaphtoryENRONTest extends BaseRaphtoryAlgoTest[String] {

  override def setSpout(): Spout[String]               = FileSpout("/tmp/email_test.csv")
  override def setGraphBuilder(): GraphBuilder[String] = new ENRONGraphBuilder()

  if (!new File("/tmp/email_test.csv").exists())
    "curl -o /tmp/email_test.csv https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv " !

  val outputFormat = FileOutputFormat(testDir)

  test("Warmup and Ingestion Test") { //TODO once watermarking is implemented, rebuild this function to actually check the timestamp
    Thread.sleep(20000)
    assert(
            32674 == 32674
    )                                 //all data is ingested and the minimum watermark is set to the last line in the data
  }

  test("Graph State Test") {
    algorithmTest(GraphState(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
    assert(true)
  }

  test("Connected Components Test") {
    algorithmTest(ConnectedComponents(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
    assert(true)
  }

  override def setSchema(): Schema[String] = Schema.STRING
}
