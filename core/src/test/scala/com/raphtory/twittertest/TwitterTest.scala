package com.raphtory.twittertest

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.spouts.StaticGraphSpout

import java.net.URL
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps

class TwitterTest extends BaseRaphtoryAlgoTest[String] {
  override val outputDirectory: String = "/tmp/raphtoryTwitterTest"

  test("Connected Components Test") {
    algorithmPointTest(ConnectedComponents(), 1400000)
      .map(assertEquals(_, "59ca85238e0c43ed8cdb4afe3a8a9248ea2c5497c945de6f4007ac4ed31946eb"))
  }

  override def batchLoading(): Boolean = true

  override def setSpout(): StaticGraphSpout = StaticGraphSpout("/tmp/twitter.csv")

  override def setGraphBuilder() = new TwitterGraphBuilder()

  def tmpFilePath = "/tmp/twitter.csv"

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(tmpFilePath -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"))

  override def munitTimeout: Duration                      = new FiniteDuration(180, TimeUnit.SECONDS)
}
