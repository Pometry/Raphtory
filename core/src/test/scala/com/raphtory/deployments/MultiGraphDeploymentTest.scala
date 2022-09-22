package com.raphtory.deployments

import cats.effect.IO
import com.raphtory.Raphtory
import com.raphtory.TestUtils
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.context.LocalContext
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.StaticGraphSpout
import munit.CatsEffectSuite

import java.net.URL
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

class MultiGraphDeploymentTest extends CatsEffectSuite {
  override val munitTimeout: Duration = 300.seconds

  val outputDirectory   = "/tmp/raphtoryTest"
  def defaultSink: Sink = FileSink(outputDirectory)

  test("Deploy two different graphs and queries work as normally") {
    val lotrPath = "/tmp/lotr.csv"

    val lotrUrl   = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv")
    val lotrFile  = TestUtils.manageTestFile(Some(lotrPath, lotrUrl))
    val lotrSpout = FileSpout(lotrPath)

    val facebookPath  = "/tmp/facebook.csv"
    val facebookUrl   = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv")
    val facebookFile  = TestUtils.manageTestFile(Some(facebookPath, facebookUrl))
    val facebookSpout = StaticGraphSpout(facebookPath)

    val files = for {
      _ <- lotrFile
      _ <- facebookFile
    } yield ()

    files
      .use { files =>
        IO.delay {
          val lotrGraph   = Raphtory.newGraph()
          lotrGraph.load(Source(lotrSpout, LOTRGraphBuilder.parse))
          val lotrTracker = lotrGraph
            .range(1, 32674, 10000)
            .window(List(500, 1000, 10000), Alignment.END)
            .execute(ConnectedComponents)
            .writeTo(defaultSink)

          val facebookGraph = Raphtory.newGraph()
          facebookGraph.load(Source(facebookSpout, FacebookGraphBuilder.parse))

          val facebookTracker = facebookGraph
            .at(88234)
            .past()
            .execute(ConnectedComponents)
            .writeTo(defaultSink)

          facebookTracker.waitForJob()
          val facebookHash = TestUtils.generateTestHash(outputDirectory, facebookTracker.getJobId)

          lotrTracker.waitForJob()
          val lotrHash = TestUtils.generateTestHash(outputDirectory, lotrTracker.getJobId)
          lotrGraph.close()
          facebookGraph.close()
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
