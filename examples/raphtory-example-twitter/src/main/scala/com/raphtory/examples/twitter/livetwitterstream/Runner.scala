package com.raphtory.examples.twitter.livetwitterstream

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.sinks.PulsarSink
import com.raphtory.twitter.builder.LiveTwitterRetweetGraphBuilder
import com.raphtory.twitter.builder.LiveTwitterUserGraphBuilder
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config

object Runner {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  def main(args: Array[String]): Unit = {
    val source  = new LiveTwitterSpout()
    val output  = PulsarSink("EdgeList")
    val builder =
      if (enableRetweetGraphBuilder)
        new LiveTwitterRetweetGraphBuilder()
      else
        new LiveTwitterUserGraphBuilder()
    val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
    graph
      .walk("10 milliseconds")
      .window("10 milliseconds")
      .execute(EdgeList())
      .writeTo(output)
  }
}
