package com.raphtory.stateTest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.output.FileOutputFormat
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover
import org.scalatest.ScalaTestVersion

import java.io.File
import scala.language.postfixOps

// Broken until we have a proper JSON spout
@DoNotDiscover
class AllCommandsTest extends BaseRaphtoryAlgoTest[String] {

  import sys.process._
  if (!new File(s"/tmp/testupdates.csv").exists())
    s"curl -o /tmp/testupdates.csv https://raw.githubusercontent.com/Raphtory/Data/main/testupdates.txt" !

  override def setSpout(): Spout[String]               = FileSpout()
  override def setGraphBuilder(): GraphBuilder[String] = new AllCommandsBuilder()
  val outputFormat                                     = FileOutputFormat(testDir)

  test("Graph State Test") {
    val results = algorithmTest(
            GraphState(),
            outputFormat,
            1,
            290001,
            10000,
            List(1000, 10000, 100000, 1000000)
    )

    logger.info(s"Graph state test finished. Result is '$results'.")

    assert(results equals "a52efc4b1962f961503ad3fabf3893e3a384056c3ce52342fa43fbdec80c08a0")
  }

  test("Connected Components Test") {
    val results = algorithmTest(
            ConnectedComponents(),
            outputFormat,
            1,
            290001,
            10000,
            List(1000, 10000, 100000, 1000000)
    )

    logger.info(s"Connected components test finished. Result is '$results'.")

    assert(results equals "ba662e8fe795c263566b1898c0f1fc6816c3f6ef5e6bbbce9db6cd06330f47a8")
  }

}
