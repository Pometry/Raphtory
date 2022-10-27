package com.raphtory

import cats.effect._
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input._
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.context.RaphtoryIOContext
import com.raphtory.sinks.FileSink
import com.typesafe.scalalogging.Logger
import munit.CatsEffectSuite
import org.slf4j.LoggerFactory

import java.net.URL
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

abstract class BaseRaphtoryAlgoTest[T: ClassTag: TypeTag](deleteResultAfterFinish: Boolean = true)
        extends CatsEffectSuite {

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  var jobId: String           = ""
  val outputDirectory: String = "/tmp/raphtoryTest"
  def defaultSink: Sink       = FileSink(outputDirectory)

  def liftFileIfNotPresent: Option[(String, URL)] = None
  def setSource(): Source

  lazy val ctx: Fixture[RaphtoryContext] = ResourceSuiteLocalFixture("context", RaphtoryIOContext.localIO())

  lazy val f: Fixture[DeployedTemporalGraph] = ResourceSuiteLocalFixture(
          "graph",
          for {
            _     <- TestUtils.manageTestFile(liftFileIfNotPresent)
            graph <- ctx()
                       .newIOGraph(failOnNotFound = false, destroy = true)
            _     <- Resource.pure(graph.load(setSource()))
          } yield graph
  )

  def graph: DeployedTemporalGraph = f()

  override def munitFixtures: Seq[Fixture[_]] = List(ctx, f)

  def algorithmTest(
      algorithm: GenericallyApplicable,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink,
      graph: DeployedTemporalGraph = graph
  ): IO[String] =
    IO {
      graph.load(setSource())

      val queryProgressTracker = graph
        .range(start, end, increment)
        .window(windows, Alignment.END)
        .execute(algorithm)
        .writeTo(sink)

      val jobId = queryProgressTracker.getJobId

      queryProgressTracker.waitForJob()

      TestUtils.generateTestHash(outputDirectory, jobId)
    }

  def algorithmPointTest(
      algorithm: GenericallyApplicable,
      timestamp: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink,
      graph: DeployedTemporalGraph = graph
  ): IO[String] =
    IO {
      graph.load(setSource())

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
