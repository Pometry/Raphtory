package com.raphtory.examples.twitter.higgsdataset

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.input.Source
import com.raphtory.examples.twitter.higgsdataset.analysis.MemberRank
import com.raphtory.examples.twitter.higgsdataset.analysis.TemporalMemberRank
import com.raphtory.examples.twitter.higgsdataset.graphbuilders.TwitterGraphBuilder
import com.raphtory.examples.twitter.livetwitterstream.Runner.enableRetweetGraphBuilder
import com.raphtory.sinks.{FileSink, PulsarSink}
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils


object Runner extends App {

  val path = "/tmp/higgs-retweet-activity.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/higgs-retweet-activity.csv"
  FileUtils.curlFile(path, url)

  // Create Graph
  val spout  = FileSpout(path)
  val graph  = Raphtory.newGraph()
  val output = FileSink("/tmp/higgsoutput")
  val builder = TwitterGraphBuilder
  val source = {
    if (enableRetweetGraphBuilder)
      Source(spout, builder.retweetParse)
    else Source(spout, builder.userParse)
  }
  graph.load(source)

    graph
      .at(1341705593)
      .past()
      .execute(PageRank)
      .writeTo(output)
      .waitForJob()

  graph.close()
}
