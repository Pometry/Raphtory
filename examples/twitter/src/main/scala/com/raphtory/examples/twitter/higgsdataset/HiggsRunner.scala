package com.raphtory.examples.twitter.higgsdataset

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.input.Source
import com.raphtory.examples.twitter.higgsdataset.analysis.MemberRank
import com.raphtory.examples.twitter.higgsdataset.analysis.TemporalMemberRank
import com.raphtory.examples.twitter.higgsdataset.graphbuilders.TwitterGraphBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

/**
  * This Runner runs PageRank, chained with MemberRank and TemporalMemberRank which are two custom written algorithms
  * to find potential bot activity in the Higgs Twitter data.
  */
object HiggsRunner extends RaphtoryApp {

  val path = "/tmp/higgs-retweet-activity.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/higgs-retweet-activity.csv"
  FileUtils.curlFile(path, url)

  override def buildContext(): RaphtoryContextType = LocalContext()

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val spout  = FileSpout(path)
      val source = Source(spout, TwitterGraphBuilder)
      val output = FileSink("/tmp/higgsoutput")

      graph.load(source)
      //get simple metrics
      graph
        .execute(Degree())
        .writeTo(output)
        .waitForJob()

      //Chained Algorithm Example
      graph
        .at(1341705593)
        .past()
        .transform(PageRank())
        .transform(MemberRank())
        .execute(TemporalMemberRank())
        .writeTo(output)
        .waitForJob()
    }
}
