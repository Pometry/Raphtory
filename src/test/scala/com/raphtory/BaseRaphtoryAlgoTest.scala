package com.raphtory

import java.io.File
import java.nio.charset.StandardCharsets
import com.google.common.hash.Hashing
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.raphtory.core.deploy.Raphtory
import com.raphtory.core.client.RaphtoryGraph
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
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.Random
import scala.reflect.runtime.universe._

abstract class BaseRaphtoryAlgoTest[T: TypeTag: ClassTag] extends AnyFunSuite with BeforeAndAfter {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  setup()

  Thread.sleep(5000)

  val spout            = setSpout()
  val graphBuilder     = setGraphBuilder()
  val graph            = Raphtory.createTypedGraph[T](spout, graphBuilder, setSchema())
  Raphtory.createClient("deployment123", Map(("raphtory.pulsar.endpoint", "localhost:1234")))
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
  def setSchema(): Schema[T]
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
    logger.debug(s"Generated hash code: '$hash'.")
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
    logger.debug(s"Generated hash code: '$hash'.")
    hash
  }

  private def getID(algorithm: GraphAlgorithm): String =
    try {
      val path = algorithm.getClass.getCanonicalName.split("\\.")
      path(path.size - 1) + "_" + System.currentTimeMillis()
    }
    catch {
      case e: NullPointerException => "Anon_Func_" + System.currentTimeMillis()
    }

}
