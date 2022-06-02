package com.raphtory.examples.twitter.livetwitterstream

import com.raphtory.algorithms.api.Alignment
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.output.{FileOutputFormat, PulsarOutputFormat}
import com.raphtory.twitter.{LiveTwitterRetweetGraphBuilder, LiveTwitterSpout, LiveTwitterUserGraphBuilder}
import com.typesafe.config.Config
import io.github.redouane59.twitter.dto.tweet.Tweet

object Runner {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  def main(args: Array[String]): Unit = {
    val source        = new LiveTwitterSpout()
    val output  = PulsarOutputFormat("EdgeList")
    val builder =
      if (enableRetweetGraphBuilder)
        new LiveTwitterRetweetGraphBuilder()
      else {
        new LiveTwitterUserGraphBuilder()
      }
    val graph        = Raphtory.stream(spout = source, graphBuilder = builder)
   graph
     .walk("10 milliseconds")
     .window("10 milliseconds")
     .execute(EdgeList())
     .writeTo(output)
  }
}
