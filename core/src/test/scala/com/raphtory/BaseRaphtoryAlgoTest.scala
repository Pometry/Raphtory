package com.raphtory

import com.google.common.hash.Hashing
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.client.GraphDeployment
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.config.PulsarController
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
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
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

abstract class BaseRaphtoryAlgoTest[T: ClassTag: TypeTag](deleteResultAfterFinish: Boolean = true)
        extends AnyFunSuite
        with BeforeAndAfter
        with BeforeAndAfterAll
        with Matchers {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  var jobId: String           = ""
  val outputDirectory: String = "/tmp/raphtoryTest"

  var graph: GraphDeployment[T]          = _
  var pulsarController: PulsarController = _
  var conf: Config                       = _
  var deploymentID: String               = _

  override def beforeAll(): Unit = {
    setup()

    val spout: Spout[T]               = setSpout()
    val graphBuilder: GraphBuilder[T] = setGraphBuilder()

    graph = Option
      .when(batchLoading())(Raphtory.batchLoadGraph[T](spout, graphBuilder))
      .fold(Raphtory.streamGraph[T](spout, graphBuilder))(identity)

    conf = graph.getConfig()
    deploymentID = conf.getString("raphtory.deploy.id")

    pulsarController = new PulsarController(conf)
  }

  override def afterAll(): Unit =
    graph.stop()

  override def withFixture(test: NoArgTest): Outcome =
    // Only clean the test directory if test succeeds
    super.withFixture(test) match {
      case failed: Failed =>
        info(s"The test '${test.name}' failed. Keeping test results for inspection.")
        failed
      case other          =>
        if (deleteResultAfterFinish)
          FileUtils
            .cleanDirectory(new File(outputDirectory + s"/$jobId"))

        other
    }

  def setSpout(): Spout[T]
  def setGraphBuilder(): GraphBuilder[T]
  def batchLoading(): Boolean
  def setup(): Unit = {}

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] =
    consumer.receive

  def algorithmTest(
      algorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long]
  ): String = {
    val queryProgressTracker =
      graph.rangeQuery(algorithm, outputFormat, start, end, increment, windows)

    jobId = queryProgressTracker.getJobId

    queryProgressTracker.waitForJob()

    generateTestHash(outputDirectory + s"/$jobId")
  }

  def algorithmPointTest(
      algorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      timestamp: Long,
      windows: List[Long] = List[Long]()
  ): String = {
    val queryProgressTracker = graph.pointQuery(algorithm, outputFormat, timestamp, windows)

    jobId = queryProgressTracker.getJobId

    queryProgressTracker.waitForJob()

    generateTestHash(outputDirectory + s"/$jobId")
  }

  private def generateTestHash(outputPath: String): String = {
    val files = new File(outputPath)
      .listFiles()
      .filter(_.isFile)

    val results = files.flatMap { file =>
      val source = scala.io.Source.fromFile(file)
      try source.getLines().toList
      catch {
        case e: Exception => throw e
      }
      finally source.close()
    }.sorted

    val hash = Hashing
      .sha256()
      .hashString(results.mkString, StandardCharsets.UTF_8)
      .toString

    logger.info(s"Generated hash code: '$hash'.")

    hash
  }
}
