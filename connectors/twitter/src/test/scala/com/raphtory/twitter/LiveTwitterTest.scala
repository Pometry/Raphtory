package com.raphtory.twitter

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.sinks.{FileSink, PulsarSink}
import com.raphtory.twitter.builder.TwitterGraphBuilder
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config
import io.github.redouane59.twitter.dto.tweet.Tweet

/**
  * To utilise this test, you must add your Twitter API credentials in application.conf under Raphtory.spout.twitter.local
  * If you would like to filter a hashtag, you can add this under Raphtory.spout.twitter.local.hashtag in application.conf
  */
object Runner extends App {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()
  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  val spout   = LiveTwitterSpout()
  val output  = FileSink("/tmp/liveTwitterStream")
  val builder = TwitterGraphBuilder
  val source = {
    if (enableRetweetGraphBuilder)
      Source(spout, builder.retweetParse)
    else Source(spout, builder.userParse)
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
