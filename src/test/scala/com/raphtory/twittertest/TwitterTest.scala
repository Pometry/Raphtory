package com.raphtory.twittertest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.spout.instance.StaticGraphSpout
import com.raphtory.output.FileOutputFormat
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

@DoNotDiscover
class TwitterTest extends BaseRaphtoryAlgoTest[String] {
  override val testDir: String = "/tmp/raphtoryTwitterTest"

  override def setup() =
    if (!new File("/tmp/twitter.csv").exists())
      if ({
        "curl -o /tmp/twitter.csv https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv " !
      } != 0) {

        logger.error("Failed to download twitter data!")

        "rm /tmp/twitter.csv" !
      }

  override def setSpout() = StaticGraphSpout("/tmp/twitter.csv")

  override def setGraphBuilder() = new TwitterGraphBuilder()

  val outputFormat = FileOutputFormat(testDir)

//sbt clean tests
  test("Connected Components Test") {
    // Finishes in ~88000ms on Avg.
    // TODO: this suicides pulsar on CI
    assert(
            algorithmPointTest(ConnectedComponents(), outputFormat, 1400000)
              equals "59ca85238e0c43ed8cdb4afe3a8a9248ea2c5497c945de6f4007ac4ed31946eb"
//        "bbb59e8ac9e5ef8aa7647016e4e40a50eaa45a7ea9a9ee506ac5b4600b96b7af"
    )

  }

  override def setSchema(): Schema[String] = Schema.STRING
}
