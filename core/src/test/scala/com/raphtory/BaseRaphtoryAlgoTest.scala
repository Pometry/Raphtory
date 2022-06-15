package com.raphtory

import cats.effect.IO
import cats.effect.kernel.Resource
import com.google.common.hash.Hashing
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.sinks.FileSink
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import munit.CatsEffectSuite
import org.apache.commons.io.FileUtils
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Failed
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

abstract class BaseRaphtoryAlgoTest[T: ClassTag: TypeTag](deleteResultAfterFinish: Boolean = true) extends CatsEffectSuite {

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  var jobId: String           = ""
  val outputDirectory: String = "/tmp/raphtoryTest"
  def defaultSink: Sink       = FileSink(outputDirectory)

  def graph: Resource[IO, DeployedTemporalGraph] =
    if (batchLoading()) Raphtory.load[T](setSpout(), setGraphBuilder())
    else Raphtory.stream[T](setSpout(), setGraphBuilder())

  val withGraph =  ResourceFixture(graph)

//  var graph: DeployedTemporalGraph     = _
//  def pulsarConnector: PulsarConnector = new PulsarConnector(conf)
//  def conf: Config                     = graph.deployment.conf
//  def deploymentID: String             = conf.getString("raphtory.deploy.id")

  override def beforeAll(): Unit =
    setup()

//    val spout: Spout[T]               = setSpout()
//    val graphBuilder: GraphBuilder[T] = setGraphBuilder()

//    graph = Option
//      .when(batchLoading())(Raphtory.load[T](spout, graphBuilder))
//      .fold(Raphtory.stream[T](spout, graphBuilder))(identity)

//  override def afterAll(): Unit = {}
//    graph.deployment.stop()

//  override def withFixture(test: NoArgTest): Outcome =
//    // Only clean the test directory if test succeeds
//    super.withFixture(test) match {
//      case failed: Failed =>
//        info(s"The test '${test.name}' failed. Keeping test results for inspection.")
//        info("Results (first 100 rows):\n" + getResults(jobId).take(100).mkString("\n"))
//        failed
//      case other          =>
//        if (deleteResultAfterFinish)
//          try {
//            val path = new File(outputDirectory + s"/$jobId")
//            FileUtils.deleteDirectory(path)
//          }
//          catch {
//            case e: Throwable =>
//              e.printStackTrace()
//          }
//        other
//    }

  def setSpout(): Spout[T]
  def setGraphBuilder(): GraphBuilder[T]
  def batchLoading(): Boolean                                               = true
  def setup(): Unit = {}

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] =
    consumer.receive

  def algorithmTest(
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

  def algorithmPointTest(
      algorithm: GenericallyApplicable,
      timestamp: Long,
      windows: List[Long] = List[Long](),
      sink: Sink = defaultSink
  )(graph: DeployedTemporalGraph): IO[String] =
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
