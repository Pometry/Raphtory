package com.raphtory

import cats.effect._
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input._
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.context.{RaphtoryContext, RaphtoryIOContext}
import com.raphtory.internals.storage.arrow.immutable
import com.raphtory.sinks.FileSink
import com.typesafe.scalalogging.Logger
import munit.CatsEffectSuite
import org.slf4j.LoggerFactory

import java.net.URL
import java.util.concurrent.TimeUnit
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

  // The context and the graph have been merged on the same fixture to prevent munit from releasing the context before the graph
  lazy val ctxAndGraph: Fixture[(RaphtoryContext, DeployedTemporalGraph)] = ResourceSuiteLocalFixture(
          "context-and-graph",
          for {
            _     <- TestUtils.manageTestFile(liftFileIfNotPresent)
//            ctx   <- RaphtoryIOContext.localIO()
            ctx   <- RaphtoryIOContext.localArrowIO[VertexProp, EdgeProp]()
            graph <- ctx.newIOGraph(failOnNotFound = false, destroy = true)
            _     <- Resource.pure(graph.load(setSource()))
          } yield (ctx, graph)
  )

  def ctx: RaphtoryContext = ctxAndGraph()._1

  def graph: DeployedTemporalGraph = ctxAndGraph()._2

  override def munitFixtures: Seq[Fixture[_]] = List(ctxAndGraph)

  def algorithmTest(
      algorithm: GenericallyApplicable,
      start: Long,
      end: Long,
      increment: Long,
      windows: Array[Long] = Array[Long](),
      sink: Sink = defaultSink,
      graph: DeployedTemporalGraph = graph
  ): IO[String] =
    IO.blocking {
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
      windows: Array[Long] = Array[Long](),
      sink: Sink = defaultSink,
      graph: DeployedTemporalGraph = graph
  ): IO[String] =
    IO.blocking {
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

case class VertexProp(
                       age: Long,
                       @immutable name: String,
                       weight: Long,
                       @immutable address_chain: String,
                       @immutable transaction_hash: String
                     )

case class EdgeProp(
                     @immutable name: String,
                     friends: Boolean,
                     weight: Long,
                     @immutable msgId: String,
                     @immutable subject: String
                   )
