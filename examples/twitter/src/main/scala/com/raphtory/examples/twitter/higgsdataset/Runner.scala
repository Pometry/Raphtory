package com.raphtory.examples.twitter.higgsdataset

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.examples.twitter.higgsdataset.analysis.{MemberRank, TemporalMemberRank}
import com.raphtory.examples.twitter.higgsdataset.graphbuilders.TwitterGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.util.Using

object Runner extends App {

  val path = "/tmp/higgs-retweet-activity.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/higgs-retweet-activity.csv"
  FileUtils.curlFile(path, url)

  // Create Graph
  val source  = FileSpout(path)
  val builder = new TwitterGraphBuilder()
  Using(Raphtory.stream(spout = source, graphBuilder = builder)) { graph =>
    graph
      .range(1341101181, 1341705593, 500000000)
      .past()
      .transform(PageRank())
      .execute(MemberRank() -> TemporalMemberRank())
      .writeTo(PulsarSink("Retweets"))
      .waitForJob()

  }
}
