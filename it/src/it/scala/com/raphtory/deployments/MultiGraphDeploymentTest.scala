package com.raphtory.deployments

import cats.effect.IO
import com.raphtory.{TestUtils, defaultConf}
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.StaticGraphSpout
import munit.CatsEffectSuite

import java.net.URL
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

/*
 *
 * The facebook dataset consists of 'circles' (or 'friends lists') from Facebook. Facebook data was collected from survey
 * participants using this Facebook app. The dataset includes node features (profiles), circles, and ego networks.
 * Facebook data has been anonymized by replacing the Facebook-internal ids for each user with a new value.
 * Also, while feature vectors from this dataset have been provided, the interpretation of those features
 * has been obscured. For instance, where the original dataset may have contained a feature "political=Democratic Party"
 * the new data would simply contain "political=anonymized feature 1". Thus, using the anonymized data it is possible
 * to determine whether two users have the same political affiliations, but not what their individual political
 *  affiliations represent.
 *
 * Dataset statistics
 * Nodes	4039
 * Edges	88234
 * Nodes in largest WCC	4039 (1.000)
 * Edges in largest WCC	88234 (1.000)
 * Nodes in largest SCC	4039 (1.000)
 * Edges in largest SCC	88234 (1.000)
 * Average clustering coefficient	0.6055
 * Number of triangles	1612010
 * Fraction of closed triangles	0.2647
 * Diameter (longest shortest path)	8
 * 90-percentile effective diameter	4.7
Note that these statistics were compiled by combining the ego-networks, including the ego nodes themselves
 *  (along with an edge to each of their friends).
 *
 * Reference: https://snap.stanford.edu/data/ego-Facebook.html
 *
 * */
class MultiGraphDeploymentTest extends CatsEffectSuite {
  override val munitTimeout: Duration = 300.seconds

  val outputDirectory   = "/tmp/raphtoryTest"
  def defaultSink: Sink = FileSink(outputDirectory)

  test("Deploy two different graphs and queries work as normally") {
    val lotrPath = "/tmp/lotr.csv"

    val lotrUrl   = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv")
    val lotrFile  = TestUtils.manageTestFile(Some(lotrPath, lotrUrl))
    val lotrSpout = FileSpout(lotrPath)

    val facebookPath = "/tmp/facebook.csv"
    val facebookUrl  = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv")
    val facebookFile = TestUtils.manageTestFile(Some(facebookPath, facebookUrl))

    val files = for {
      _ <- lotrFile
      _ <- facebookFile
    } yield ()

    files
      .use { files =>
        IO.delay {
          val ctx       = new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
          val lotrJobId = ctx.runWithNewGraph() { lotrGraph =>
            lotrGraph.load(CSVEdgeListSource(lotrSpout))
            val lotrTracker = lotrGraph
              .range(1, 32674, 10000)
              .window(List(500, 1000, 10000), Alignment.END)
              .execute(ConnectedComponents)
              .writeTo(defaultSink)
            lotrTracker.waitForJob()
            lotrTracker.getJobId
          }

          val facebookJobId = ctx.runWithNewGraph() { facebookGraph =>
            facebookGraph.load(CSVEdgeListSource(StaticGraphSpout(facebookPath), delimiter = " "))
            val facebookTracker = facebookGraph
              .at(88234)
              .past()
              .execute(ConnectedComponents)
              .writeTo(defaultSink)

            facebookTracker.waitForJob()
            facebookTracker.getJobId
          }

          val lotrHash     = TestUtils.generateTestHash(outputDirectory, lotrJobId)
          val facebookHash = TestUtils.generateTestHash(outputDirectory, facebookJobId)

          (lotrHash, facebookHash)
        }
      }
      .map {
        case (lotrHash, facebookHash) =>
          assertEquals(lotrHash, "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410")
          assertEquals(facebookHash, "96e9415d7b657e0c306021bfa55daa9d5507271ccff2390894e16597470cb4ab")
      }
  }
}
