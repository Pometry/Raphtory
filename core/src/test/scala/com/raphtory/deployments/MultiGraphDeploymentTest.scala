package com.raphtory.deployments

import cats.effect.IO
import com.raphtory.Raphtory
import com.raphtory.TestUtils
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.output.sink.Sink
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.StaticGraphSpout
import com.raphtory.facebooktest.FacebookGraphBuilder
import munit.CatsEffectSuite

import java.net.URL

class MultiGraphDeploymentTest extends CatsEffectSuite {
  val outputDirectory   = "/tmp/raphtoryTest"
  def defaultSink: Sink = FileSink(outputDirectory)

  test("Deploy two different graphs and queries work as normally") {
    val lotrPath    = "/tmp/lotr.csv"
    val lotrUrl     = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv")
    val lotrFile    = TestUtils.manageTestFile(Some(lotrPath, lotrUrl))
    val lotrSpout   = FileSpout(lotrPath)
    val lotrBuilder = new LOTRGraphBuilder()

    val facebookPath    = "/tmp/facebook.csv"
    val facebookUrl     = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv")
    val facebookFile    = TestUtils.manageTestFile(Some(facebookPath, facebookUrl))
    val facebookSpout   = StaticGraphSpout(facebookPath)
    val facebookBuilder = new FacebookGraphBuilder()

    val files = for {
      _ <- lotrFile
      _ <- facebookFile
    } yield ()

    files
      .use { files =>
        IO.delay {
          val lotrGraph   = Raphtory.stream(lotrSpout, lotrBuilder)
          val lotrTracker = lotrGraph
            .range(1, 32674, 10000)
            .window(List(500, 1000, 10000), Alignment.END)
            .execute(ConnectedComponents())
            .writeTo(defaultSink)

          val facebookGraph   = Raphtory.stream(facebookSpout, facebookBuilder)
          val facebookTracker = facebookGraph
            .at(88234)
            .past()
            .execute(ConnectedComponents())
            .writeTo(defaultSink)

          lotrTracker.waitForJob()
          val lotrHash = TestUtils.generateTestHash(outputDirectory, lotrTracker.getJobId)

          facebookTracker.waitForJob()
          val facebookHash = TestUtils.generateTestHash(outputDirectory, facebookTracker.getJobId)

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
