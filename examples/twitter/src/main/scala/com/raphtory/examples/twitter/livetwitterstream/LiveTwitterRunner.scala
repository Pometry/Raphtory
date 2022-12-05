package com.raphtory.examples.twitter.livetwitterstream

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.twitter.builder.TwitterRetweetGraphBuilder
import com.raphtory.twitter.builder.TwitterUserGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config

object LiveTwitterRunner extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val raphtoryConfig: Config             = ConfigBuilder.getDefaultConfig
      val enableRetweetGraphBuilder: Boolean =
        raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

      val spout  = LiveTwitterSpout()
      val output = FileSink("/tmp/liveTwitterStream")

      val source = {
        if (enableRetweetGraphBuilder)
          Source(spout, TwitterRetweetGraphBuilder)
        else Source(spout, TwitterUserGraphBuilder)
      }

      graph.load(source)

      graph
        .walk("10 milliseconds")
        .dateWindow("10 milliseconds")
        .execute(EdgeList())
        .writeTo(output)
        .waitForJob()
    }
}
