package com.raphtory.twitter

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.twitter.builder.TwitterRetweetGraphBuilder
import com.raphtory.twitter.builder.TwitterUserGraphBuilder
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config

/**
  * To utilise this test, you must add your Twitter API credentials in application.conf under Raphtory.spout.twitter.local
  * If you would like to filter a hashtag, you can add this under Raphtory.spout.twitter.local.hashtag in application.conf
  */
object LiveTwitterTest extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val raphtoryConfig: Config = ConfigBuilder().build().getConfig

      val enableRetweetGraphBuilder: Boolean =
        raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

      val spout = LiveTwitterSpout()

      val graphBuilder =
        if (enableRetweetGraphBuilder)
          TwitterUserGraphBuilder
        else
          TwitterRetweetGraphBuilder

      val source = Source(spout, graphBuilder)
      val output = FileSink("/tmp/output")

      graph.load(source)
      graph
        .walk("10 milliseconds")
        .window("10 milliseconds")
        .execute(EdgeList())
        .writeTo(output)
        .waitForJob()
    }
}
