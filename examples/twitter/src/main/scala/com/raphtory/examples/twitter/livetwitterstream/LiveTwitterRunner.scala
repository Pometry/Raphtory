package com.raphtory.examples.twitter.livetwitterstream

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.twitter.builder.TwitterRetweetGraphBuilder
import com.raphtory.twitter.builder.TwitterUserGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config

object LiveTwitterRunner extends App {

    val raphtoryConfig: Config = Raphtory.getDefaultConfig()
    val enableRetweetGraphBuilder: Boolean =
      raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

    val spout   = LiveTwitterSpout()
    val output  = FileSink("/tmp/liveTwitterStream")

    val source = {
      if (enableRetweetGraphBuilder)
      Source(spout, TwitterRetweetGraphBuilder)
      else Source(spout, TwitterUserGraphBuilder)
    }
    val graph  = Raphtory.newGraph()

        graph.load(source)

        graph
          .walk("10 milliseconds")
          .window("10 milliseconds")
          .execute(EdgeList())
          .writeTo(output)
          .waitForJob()

        graph.close()
}
