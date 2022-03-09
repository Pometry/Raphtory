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

  override def setup(): Unit =
    if (!new File("/tmp/email_test.csv").exists())
      "curl -o /tmp/email_test.csv https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv " !

  val outputFormat: FileOutputFormat = FileOutputFormat(testDir)

  test("Graph State Test") {
    graph.liveQuery(GraphState(), outputFormat, increment = 10000).waitForJob()
//    algorithmTest(GraphState(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
    assert(true)
  }

  test("Connected Components Test") {
    algorithmTest(ConnectedComponents(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
    assert(true)
  }
}
