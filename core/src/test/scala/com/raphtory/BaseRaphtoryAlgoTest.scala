package com.raphtory

import cats.effect.IO
import cats.effect.SyncIO
import cats.effect.kernel.Resource
import com.google.common.hash.Hashing
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.api.output.sink.Sink
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
    if (batchLoading()) Raphtory.load[T](setSpout(), setGraphBuilder())
    else Raphtory.stream[T](setSpout(), setGraphBuilder())

  val withGraph: SyncIO[FunFixture[DeployedTemporalGraph]] = ResourceFixture(
          for {
            _ <- manageTestFile
            g <- graph
          } yield g
  )

  val suiteGraph: Fixture[DeployedTemporalGraph] = ResourceSuiteLocalFixture(
          "graph",
          for {
            _ <- manageTestFile
            g <- graph
          } yield g
  )

  def graphS = suiteGraph()

  override def munitFixtures = List(suiteGraph)

  private def manageTestFile: Resource[IO, Any] =
    liftFileIfNotPresent match {
      case None           => Resource.eval(IO.unit)
      case Some((p, url)) =>
        val path = Paths.get(p)
        Resource.make(IO.blocking(if (Files.notExists(path)) s"curl -o $path $url" !!))(_ =>
          IO.blocking { // this is a bit hacky but it allows us
            Runtime.getRuntime.addShutdownHook(new Thread {
              override def run(): Unit =
                Files.deleteIfExists(path)
            })
          }
        )
    }

  def liftFileIfNotPresent: Option[(String, URL)] = None

  def setSpout(): Spout[T]
  def setGraphBuilder(): GraphBuilder[T]
  def batchLoading(): Boolean = true

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] =
    consumer.receive

  private def algorithmTestInternal(
      algorithm: GenericallyApplicable,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink
  )(graph: DeployedTemporalGraph): IO[String] =
    IO {
      val queryProgressTracker = graph
        .range(start, end, increment)
        .window(windows, Alignment.END)
        .execute(algorithm)
        .writeTo(sink)

      val jobId = queryProgressTracker.getJobId

      queryProgressTracker.waitForJob()

      generateTestHash(jobId)
    }

  def algorithmTest(
      algorithm: GenericallyApplicable,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink,
      graph: DeployedTemporalGraph = graphS
  ): IO[String] =
    algorithmTestInternal(algorithm, start, end, increment, windows, sink)(graph)

  def algorithmPointTest(
      algorithm: GenericallyApplicable,
      timestamp: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink,
      graph: DeployedTemporalGraph = graphS
  ): IO[String] =
    IO {
      val queryProgressTracker = graph
        .at(timestamp)
        .window(windows, Alignment.END)
        .execute(algorithm)
        .writeTo(sink)

      val jobId = queryProgressTracker.getJobId

      queryProgressTracker.waitForJob()

      generateTestHash(jobId)
    }

  def resultsHash(results: IterableOnce[String]): String =
    Hashing
      .sha256()
      .hashString(results.iterator.toSeq.sorted.mkString, StandardCharsets.UTF_8)
      .toString

  def getResults(jobID: String = jobId): Iterator[String] = {
    val files = new File(outputDirectory + "/" + jobID)
      .listFiles()
      .filter(_.isFile)

    files.iterator.flatMap { file =>
      val source = scala.io.Source.fromFile(file)
      try source.getLines().toList
      catch {
        case e: Exception => throw e
      }
      finally source.close()
    }
  }

  private def generateTestHash(jobId: String = jobId): String = {
    val results = getResults(jobId)
    val hash    = resultsHash(results)
    logger.info(s"Generated hash code: '$hash'.")

    hash
  }
}
