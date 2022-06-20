package com.raphtory.examples.twitter.higgsdataset

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.examples.twitter.higgsdataset.analysis.MemberRank
import com.raphtory.examples.twitter.higgsdataset.analysis.TemporalMemberRank
import com.raphtory.examples.twitter.higgsdataset.graphbuilders.TwitterGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object Runner extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    val path = "/tmp/higgs-retweet-activity.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/higgs-retweet-activity.csv"
    FileUtils.curlFile(path, url)

    // Create Graph
    val source  = FileSpout(path)
    val builder = new TwitterGraphBuilder()
    Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
      IO {
        graph
          .range(1341101181, 1341705593, 500000000)
          .past()
          .transform(PageRank())
          .execute(MemberRank() -> TemporalMemberRank())
          .writeTo(PulsarSink("Retweets"))
          .waitForJob()
        ExitCode.Success
      }
    }
  }
}
