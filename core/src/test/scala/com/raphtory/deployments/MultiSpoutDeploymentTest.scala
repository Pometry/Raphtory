package com.raphtory.deployments

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.Raphtory
import com.raphtory.TestUtils
import com.raphtory.api.output.sink.Sink
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import munit.CatsEffectSuite
import org.apache.commons.io.FileUtils

import java.io.File
import java.io.FileWriter
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths
import scala.sys.process._

class MultiSpoutDeploymentTest extends CatsEffectSuite {
  val outputDirectory   = "/tmp/raphtoryTest"
  def defaultSink: Sink = FileSink(outputDirectory)

  test("Deploy a graph from two spouts") {
    val url          = new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv")
    val path         = "/tmp/lotr.csv"
    val splitDir     = "/tmp/lotr-split"
    val oddPath      = s"$splitDir/odd.csv"
    val evenPath     = s"$splitDir/even.csv"
    val fileDownload = TestUtils.manageTestFile(Some(path, url))
    val oddSpout     = FileSpout(oddPath)
    val evenSpout    = FileSpout(evenPath)
    val lotrBuilder  = new LOTRGraphBuilder()

    val files = for {
      _        <- fileDownload
      file     <- Resource.fromAutoCloseable(IO(scala.io.Source.fromFile(path)))
      _        <- Resource.make(IO(s"mkdir -p $splitDir" !!))(path => IO(s"rm -r $splitDir" !!))
      oddFile  <- Resource.fromAutoCloseable(IO(new FileWriter(oddPath)))
      evenFile <- Resource.fromAutoCloseable(IO(new FileWriter(evenPath)))
    } yield (file, oddFile, evenFile)

    files
      .use {
        case (file, oddFile, evenFile) =>
          IO.delay {
            file.getLines().grouped(2).foreach { linePair =>
              val oddLine = linePair.head
              oddFile.write(s"$oddLine\n")
              if (linePair.size == 2) {
                val evenLine = linePair.last
                evenFile.write(s"$evenLine\n")
              }
            }
            Seq(oddFile, evenFile).foreach(_.flush())

            val graph   = Raphtory.localContext().newGraph()
            graph.ingest(Source(oddSpout, lotrBuilder))
            graph.ingest(Source(evenSpout, lotrBuilder))
            val tracker = graph
              .range(1, 32674, 10000)
              .window(List(500, 1000, 10000), Alignment.END)
              .execute(ConnectedComponents())
              .writeTo(defaultSink)

            tracker.waitForJob()
            graph.close()
            TestUtils.generateTestHash(outputDirectory, tracker.getJobId)
          }
      }
      .map(hash => assertEquals(hash, "c6c26df04212ac7c0ba352d3acd79fb2c38f2c2943079bbe48dde9ea2b399410"))
  }
}
