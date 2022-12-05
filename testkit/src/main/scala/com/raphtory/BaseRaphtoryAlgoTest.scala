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
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.attribute.PosixFilePermissions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

abstract class BaseRaphtoryAlgoTest[T: ClassTag: TypeTag](deleteResultAfterFinish: Boolean = true)
        extends CatsEffectSuite {

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  var jobId: String           = ""
  val outputDirectory: String = Option(System.getenv("RAPHTORY_ITEST_PATH")).getOrElse("/tmp/raphtoryTest")
  def defaultSink: Sink       = FileSink("/tmp/raphtoryTest")

  lazy val raphtoryData =
    Option(System.getenv("RAPHTORY_ITEST_PATH")).getOrElse(
            Files
              .createTempDirectory("tests" )
              .toString
    )


  def resolveSpout(fileName: String): String =
    if (System.getenv("RAPHTORY_ITEST_PATH") != null)
      fileName
    else {
      Paths.get(raphtoryData, fileName).toString
    }

  def tmpLocation(fileName: String): String = {
    Paths.get(raphtoryData, fileName).toString
  }

  //  def
  def liftFileIfNotPresent: Option[(String, URL)] = None
  def setSource(): Source

  // The context and the graph have been merged on the same fixture to prevent munit from releasing the context before the graph
  lazy val ctxAndGraph: Fixture[(RaphtoryContext, DeployedTemporalGraph)] = ResourceSuiteLocalFixture(
          "context-and-graph",
          for {
            _     <- TestUtils.manageTestFile(liftFileIfNotPresent)
            ctx   <- Option(System.getenv("RAPHTORY_ITEST_PATH")) // if RAPHTORY_ITEST_PATH is set then use the remote context
                       .map { _ =>
                         logger.warn("!! Running Integration Tests on Remote Raphtory !!")
                         RaphtoryIOContext.remoteIO()
                       }
                       .getOrElse(RaphtoryIOContext.localArrowIO[VertexProp, EdgeProp]()/*RaphtoryIOContext.localIO()*/)
            graph <- ctx.newIOGraph(failOnNotFound = false, destroy = true)
            _     <- Resource.pure(graph.load(setSource()))
          } yield (ctx, graph)
  )

  def runningIntegrationTest: Boolean = Option(System.getenv("RAPHTORY_ITEST_PATH")).isDefined

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
