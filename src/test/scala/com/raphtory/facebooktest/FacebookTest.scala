package com.raphtory.facebooktest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.SpoutExecutor
import org.apache.pulsar.client.api.Schema
import com.raphtory.output.FileOutputFormat
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.spouts.StaticGraphSpout

import java.io.File
import scala.language.postfixOps
import scala.sys.process._
import org.scalatest._

@DoNotDiscover
class FacebookTest extends BaseRaphtoryAlgoTest[String] {

  override def batchLoading(): Boolean = false

  override def setup()           =
    if (!new File("/tmp/facebook.csv").exists())
      "curl -o /tmp/facebook.csv https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv " !
  override def setSpout()        = StaticGraphSpout("/tmp/facebook.csv")
  override def setGraphBuilder() = new FacebookGraphBuilder()
  val outputFormat               = FileOutputFormat(testDir)

////sbt clean tests
  test("Connected Components Test") {
    assert(
            // Finishes in 31039ms on Avg.
            algorithmPointTest(ConnectedComponents(), outputFormat, 88234)
              equals "96e9415d7b657e0c306021bfa55daa9d5507271ccff2390894e16597470cb4ab"
    )
  }
}
