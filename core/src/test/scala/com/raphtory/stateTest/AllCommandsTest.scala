package com.raphtory.stateTest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.Spout
import com.raphtory.internals.graph.GraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

import java.net.URL
import scala.language.postfixOps

// Broken until we have a proper JSON spout
@munit.IgnoreSuite
class AllCommandsTest extends BaseRaphtoryAlgoTest[String] {
  test("Graph State Test".ignore) {
    val sink = FileSink(outputDirectory)

    algorithmTest(
            algorithm = GraphState(),
            sink = sink,
            start = 1,
            end = 290001,
            increment = 10000,
            windows = List(1000, 10000, 100000, 1000000)
    ).map { result =>
      logger.info(s"Graph state test finished. Result is '$result'.")
      assertEquals(result, "a52efc4b1962f961503ad3fabf3893e3a384056c3ce52342fa43fbdec80c08a0")
    }
  }

  test("Connected Components Test".ignore) {
    val sink = FileSink(outputDirectory)

    algorithmTest(
            algorithm = ConnectedComponents,
            sink = sink,
            start = 1,
            end = 290001,
            increment = 10000,
            windows = List(1000, 10000, 100000, 1000000)
    ).map { result =>
      logger.info(s"Connected components test finished. Result is '$result'.")
      assertEquals(result, "ba662e8fe795c263566b1898c0f1fc6816c3f6ef5e6bbbce9db6cd06330f47a8")
    }

  }
  override def setSpout(): Spout[String] = FileSpout("/tmp/testupdates.csv")

  override def setGraphBuilder(): GraphBuilder[String] = new AllCommandsBuilder()

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(("/tmp/testupdates.csv", new URL("https://raw.githubusercontent.com/Raphtory/Data/main/testupdates.txt")))
}
