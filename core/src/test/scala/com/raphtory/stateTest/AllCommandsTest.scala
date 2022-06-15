package com.raphtory.stateTest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.GraphState
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import org.scalatest.DoNotDiscover

import sys.process._
import java.io.File
import scala.language.postfixOps

// Broken until we have a proper JSON spout
@DoNotDiscover
class AllCommandsTest extends BaseRaphtoryAlgoTest[String] {
//  test("Graph State Test") {
//    val sink = FileSink(outputDirectory)
//
//    val results = algorithmTest(
//            algorithm = GraphState(),
//            sink = sink,
//            start = 1,
//            end = 290001,
//            increment = 10000,
//            windows = List(1000, 10000, 100000, 1000000)
//    )
//
//    logger.info(s"Graph state test finished. Result is '$results'.")
//
//    val expected = "a52efc4b1962f961503ad3fabf3893e3a384056c3ce52342fa43fbdec80c08a0"
//
//    assert(results equals expected)
//  }
//
//  test("Connected Components Test") {
//    val sink = FileSink(outputDirectory)
//
//    val results = algorithmTest(
//            algorithm = ConnectedComponents(),
//            sink = sink,
//            start = 1,
//            end = 290001,
//            increment = 10000,
//            windows = List(1000, 10000, 100000, 1000000)
//    )
//
//    logger.info(s"Connected components test finished. Result is '$results'.")
//
//    val expected = "ba662e8fe795c263566b1898c0f1fc6816c3f6ef5e6bbbce9db6cd06330f47a8"
//
//    assert(results equals "ba662e8fe795c263566b1898c0f1fc6816c3f6ef5e6bbbce9db6cd06330f47a8")
//  }
//
  override def setSpout(): Spout[String] = FileSpout()

  override def setGraphBuilder(): GraphBuilder[String] = new AllCommandsBuilder()
//
//  override def setup(): Unit = {
//    val path = "/tmp/testupdates.csv"
//    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/testupdates.txt"
//
//    if (!new File(path).exists())
//      try s"curl -o $path $url" !!
//      catch {
//        case ex: Exception =>
//          logger.error(s"Failed to download 'testupdates.csv' due to ${ex.getMessage}.")
//          ex.printStackTrace()
//
//          (s"rm $path" !)
//          throw ex
//      }
//  }
}
