package com.raphtory.facebooktest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.components.spout.SpoutExecutor
import org.apache.pulsar.client.api.Schema
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.StaticGraphSpout

import java.io.File
import scala.language.postfixOps
import scala.sys.process._
import org.scalatest._

@DoNotDiscover
class FacebookTest extends BaseRaphtoryAlgoTest[String] {

  withGraph.test("Connected Components Test") { graph =>
    val sink = FileSink(outputDirectory)

    val result = algorithmPointTest(
            algorithm = ConnectedComponents(),
            sink = sink,
            timestamp = 88234
    )(graph).unsafeRunSync()

    val expected = "96e9415d7b657e0c306021bfa55daa9d5507271ccff2390894e16597470cb4ab"

    assertEquals(result, expected)
  }

  override def batchLoading(): Boolean = false

  override def setSpout(): StaticGraphSpout = StaticGraphSpout("/tmp/facebook.csv")

  override def setGraphBuilder() = new FacebookGraphBuilder()

  override def setup(): Unit = {
    val path = "/tmp/facebook.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv"

    if (!new File(path).exists())
      try s"curl -o $path $url" !!
      catch {
        case ex: Exception =>
          logger.error(s"Failed to download 'facebook.csv' due to ${ex.getMessage}.")
          ex.printStackTrace()

          (s"rm $path" !)
          throw ex
      }
  }
}
