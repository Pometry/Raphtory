package com.raphtory.twittertest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.components.spout.SpoutExecutor
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.StaticGraphSpout
import org.apache.pulsar.client.api.Schema
import org.scalatest.DoNotDiscover

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

class TwitterTest extends BaseRaphtoryAlgoTest[String] {
  override val outputDirectory: String = "/tmp/raphtoryTwitterTest"

//  test("Connected Components Test") {
//    assert(
//            algorithmPointTest(ConnectedComponents(), 1400000)
//              equals "59ca85238e0c43ed8cdb4afe3a8a9248ea2c5497c945de6f4007ac4ed31946eb"
//    )
//  }

  override def batchLoading(): Boolean = true

  override def setSpout(): StaticGraphSpout = StaticGraphSpout("/tmp/twitter.csv")

  override def setGraphBuilder() = new TwitterGraphBuilder()

  override def setup(): Unit = {
    val path = "/tmp/twitter.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"

    if (!new File(path).exists())
      try s"curl -o $path $url" !!
      catch {
        case ex: Exception =>
          logger.error(s"Failed to download 'twitter.csv' due to ${ex.getMessage}.")
          ex.printStackTrace()

          (s"rm $path" !)
          throw ex
      }
  }
}
