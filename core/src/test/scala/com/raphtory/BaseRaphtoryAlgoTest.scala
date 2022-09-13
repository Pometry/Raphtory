package com.raphtory

import cats.effect.IO
import cats.effect.SyncIO
import cats.effect.kernel.Resource
import com.google.common.hash.Hashing
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.graph.GraphBuilder
import com.raphtory.sinks.FileSink
import com.typesafe.scalalogging.Logger
import munit.CatsEffectSuite
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.sys.process._

abstract class BaseRaphtoryAlgoTest[T: ClassTag: TypeTag](deleteResultAfterFinish: Boolean = true)
        extends CatsEffectSuite {

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  var jobId: String           = ""
  val outputDirectory: String = "/tmp/raphtoryTest"
  def defaultSink: Sink       = FileSink(outputDirectory)

  private def graph: Resource[IO, DeployedTemporalGraph] =
    Raphtory.newIOGraph()

  lazy val withGraph: SyncIO[FunFixture[TemporalGraph]] = ResourceFixture(
          for {
            _ <- TestUtils.manageTestFile(liftFileIfNotPresent)
            g <- graph
            _  = g.load(Source(setSpout(), setGraphBuilder()))
          } yield g
  )

  lazy val suiteGraph: Fixture[DeployedTemporalGraph] = ResourceSuiteLocalFixture(
          "graph",
          for {
            _ <- TestUtils.manageTestFile(liftFileIfNotPresent)
            g <- graph
            _  = g.load(Source(setSpout(), setGraphBuilder()))
          } yield g
  )

  def graphS: DeployedTemporalGraph = suiteGraph()

  override def munitFixtures = List(suiteGraph)

  def liftFileIfNotPresent: Option[(String, URL)] = None

  def setSpout(): Spout[T]
  def setGraphBuilder(): (Graph, T) => Unit

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] =
    consumer.receive

  private def algorithmTestInternal(
      algorithm: GenericallyApplicable,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink
  )(graph: TemporalGraph): IO[String] =
    IO {
      val queryProgressTracker = graph
        .range(start, end, increment)
        .window(windows, Alignment.END)
        .execute(algorithm)
        .writeTo(sink)

      val jobId = queryProgressTracker.getJobId

      queryProgressTracker.waitForJob()

      TestUtils.generateTestHash(outputDirectory, jobId)
    }

  def algorithmTest(
      algorithm: GenericallyApplicable,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink,
      graph: TemporalGraph = graphS
  ): IO[String] =
    algorithmTestInternal(algorithm, start, end, increment, windows, sink)(graph)

  def algorithmPointTest(
      algorithm: GenericallyApplicable,
      timestamp: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink,
      graph: TemporalGraph = graphS
  ): IO[String] =
    IO {
      val queryProgressTracker = graph
        .at(timestamp)
        .window(windows, Alignment.END)
        .execute(algorithm)
        .writeTo(sink)

      val jobId = queryProgressTracker.getJobId

      queryProgressTracker.waitForJob()

      TestUtils.generateTestHash(outputDirectory, jobId)
    }
}
