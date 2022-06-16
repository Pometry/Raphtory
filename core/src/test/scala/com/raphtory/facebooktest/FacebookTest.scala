package com.raphtory.facebooktest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.StaticGraphSpout
import org.scalatest._

import java.net.URL
import scala.language.postfixOps

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

  def tempFilePath = "/tmp/facebook.csv" // FIXME this is not great should use Files.createTempFile()

  override def setSpout(): StaticGraphSpout = StaticGraphSpout(tempFilePath)

  override def setGraphBuilder() = new FacebookGraphBuilder()

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some((tempFilePath, new URL("https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv")))

}
