package com.raphtory.twittertest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.output.FileOutputFormat
import com.raphtory.spouts.StaticGraphSpout
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

@DoNotDiscover
class TwitterTest extends BaseRaphtoryAlgoTest[String] {
  override val testDir: String         = "/tmp/raphtoryTwitterTest"
  override def batchLoading(): Boolean = true

  override def setup(): Unit = {
    val path = "/tmp/twitter.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"

    val file = new File(path)
    if (!file.exists())
      s"curl -o $path $url " !
  }

  override def setSpout() = StaticGraphSpout("/tmp/twitter.csv")

  override def setGraphBuilder() = new TwitterGraphBuilder()

  val outputFormat = FileOutputFormat(testDir)

  test("Connected Components Test") {
    // Finishes in ~88000ms on Avg.
    // TODO: this suicides pulsar on CI
    assert(
            algorithmPointTest(ConnectedComponents(), outputFormat, 1400000)
              equals "59ca85238e0c43ed8cdb4afe3a8a9248ea2c5497c945de6f4007ac4ed31946eb"
    )
  }
}
