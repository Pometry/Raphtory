package com.raphtory

import java.io.File
import java.nio.charset.StandardCharsets
import com.google.common.hash.Hashing
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.components.spout.SpoutExecutor
import com.raphtory.config.PulsarController
import com.raphtory.deployment.Raphtory
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import org.scalactic.source
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.Random
import scala.reflect.runtime.universe._

abstract class BaseRaphtoryAlgoTest[T: ClassTag: TypeTag]
        extends AnyFunSuite
        with BeforeAndAfterAll {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  setup()

  Thread.sleep(5000)

  val spout        = setSpout()
  val graphBuilder = setGraphBuilder()

  val graph            =
    if (batchLoading) Raphtory.batchLoadGraph[T](spout, graphBuilder)
    else Raphtory.streamGraph[T](spout, graphBuilder)
  val conf             = graph.getConfig()
  val pulsarController = new PulsarController(conf)

  val pulsarAddress: String =
    conf.getString("raphtory.pulsar.broker.address") //conf.getString("Raphtory.pulsarAddress")
  val deploymentID: String = conf.getString("raphtory.deploy.id")

  private val kryo: PulsarKryoSerialiser   = PulsarKryoSerialiser()
  implicit val schema: Schema[Array[Byte]] = Schema.BYTES
  val testDir                              = "/tmp/raphtoryTest" //TODO CHANGE TO USER PARAM

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
    val startingTime         = System.currentTimeMillis()
    val queryProgressTracker =
      graph.rangeQuery(algorithm, outputFormat, start, end, increment, windows)
    val jobId                = queryProgressTracker.getJobId()
    queryProgressTracker.waitForJob()

    val dir     = new File(testDir + s"/$jobId").listFiles
      .filter(_.isFile)
    val results =
      (for (i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted.flatten
    val hash    = Hashing.sha256().hashString(new String(results), StandardCharsets.UTF_8).toString
    logger.info(s"Generated hash code: '$hash'.")
    hash
  }

  def algorithmPointTest(
      algorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      timestamp: Long,
      windows: List[Long] = List[Long]()
  ): String = {
    val startingTime         = System.currentTimeMillis()
    val queryProgressTracker = graph.pointQuery(algorithm, outputFormat, timestamp, windows)
    val jobId                = queryProgressTracker.getJobId()
    queryProgressTracker.waitForJob()

    val dir     = new File(testDir + s"/$jobId").listFiles.filter(_.isFile)
    val results =
      (for (i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted.flatten
    val hash    = Hashing.sha256().hashString(new String(results), StandardCharsets.UTF_8).toString
    logger.info(s"Generated hash code: '$hash'.")
    hash
  }

  override def afterAll(): Unit =
    graph.stop()
}
