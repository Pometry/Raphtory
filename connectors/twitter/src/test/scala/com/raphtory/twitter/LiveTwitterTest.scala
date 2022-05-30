package com.raphtory.twitter

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.output.{FileOutputFormat, PulsarOutputFormat}
import com.raphtory.twitter.{LiveTwitterRetweetGraphBuilder, LiveTwitterSpout, LiveTwitterUserGraphBuilder}
import com.typesafe.config.Config
import io.github.redouane59.twitter.dto.tweet.Tweet

/**
 * To utilise this test, you must add your Twitter API credentials in application.conf under Raphtory.spout.twitter.local
 * If you would like to filter a hashtag, you can add this under Raphtory.spout.twitter.local.hashtag in application.conf
 */
object LiveTwitterTest {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  def main(args: Array[String]): Unit = {
    val spout        = new LiveTwitterSpout()
    val graphBuilder =
      if (enableRetweetGraphBuilder)
        new LiveTwitterRetweetGraphBuilder()
      else {
        new LiveTwitterUserGraphBuilder()
      }
    val graph        = Raphtory.stream[Tweet](spout, graphBuilder)
    graph
      .walk("5 milliseconds")
      .window("5 milliseconds")
      .execute(EdgeList())
      .writeTo(PulsarOutputFormat("EdgeList1"))
  }
}

