package com.raphtory

import com.raphtory.MyTest.controller
import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.components.graphbuilder.Properties
import com.raphtory.core.components.graphbuilder.VertexAdd
import com.raphtory.core.components.partition.Writer
import com.raphtory.core.config.PulsarController
import com.raphtory.core.deploy.Raphtory
import com.raphtory.core.storage.pojograph.PojoBasedPartition
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import monix.execution.UncaughtExceptionReporter
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Random

object MyTest extends App {
  final val spoutTopic   = "test_spout_topic"
  final val builderTopic = "test_graph_topic"

  val config: Config = Raphtory.getDefaultConfig(
          Map[String, Any](
                  ("raphtory.spout.topic", spoutTopic),
                  ("raphtory.partitions.serverCount", 1),
                  ("raphtory.partitions.countPerServer", 1),
                  ("raphtory.deploy.id", builderTopic)
          )
  )

  val controller = new PulsarController(config)
  val fileSpout  = new FileSpout(controller, topic = spoutTopic)

  val scheduler = new MonixScheduler().scheduler

  scheduler.execute {
    fileSpout
  }

  scheduler.execute(
          new GraphBuilder(controller, readTopic = spoutTopic, outputTopic = builderTopic)
  )

  // Controlled writer
  //scheduler.execute(new Writer(controller, topic = builderTopic))

  val storage = new PojoBasedPartition(1, config)

  scheduler.execute(
          new com.raphtory.core.components.partition.Writer(1, storage, config, controller)
  )

}

case class SpoutModel(src: String, dst: String)

class FileSpout(controller: PulsarController, topic: String) extends Runnable {
  val logger: Logger        = Logger(LoggerFactory.getLogger(this.getClass))
  private val spoutProducer = controller.createProducer(Schema.BYTES, topic)

  var messagesProcessed: Int = _

  def run(): Unit = {
    messagesProcessed = 0

    val path    = "/tmp/twitter.csv"
    val dataUrl = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"
    downloadData(url = dataUrl, outputPath = path)

    Thread.sleep(5000)

    val fileContent = readFile(file = path)

    fileContent.foreach { line =>
      val data = line.getBytes()

      spoutProducer.sendAsync(data)

      messagesProcessed = messagesProcessed + 1

      if (messagesProcessed % 100_000 == 0)
        logger.info(s"File spout: Sent $messagesProcessed so far.")
    }
  }

  def readFile(file: String): ArrayBuffer[String] = {
    val content = ArrayBuffer.empty[String]

    val bufferedSource = Source.fromFile(file)
    for (line <- bufferedSource.getLines)
      content.addOne(line)
    bufferedSource.close

    logger.info(s"Finished reading data, processed ${content.size} items.")

    content
  }

  def downloadData(url: String, outputPath: String): Unit = {
    val status = s"curl -o $outputPath $url" !

    if (status != 0) {
      logger.error("Failed to download twitter data!")
      System.exit(-1)
    }
    else {
      logger.info("Twitter data successfully downloaded.")
      "wc -l /tmp/twitter.csv " !
    }

  }
}

case class MyUpdate(src: Long, dst: Long)

class GraphBuilder(controller: PulsarController, readTopic: String, outputTopic: String)
        extends Runnable {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  controller.createListeningConsumer(
          subscriptionName = "graph-builder",
          messageListener = messageListener(),
          schema = Schema.BYTES,
          topics = readTopic
  )

  private val graphProducer = controller.createProducer(Schema.BYTES, outputTopic)
  private val kryo          = PulsarKryoSerialiser()

  var messagesProcessed = 0

  def run(): Unit = {}

  def processTuple(line: String): Unit = {
    val fileLine = line.split(" ").map(_.trim)

    val src = fileLine(0).toLong
    val dst = fileLine(1).toLong

    val message = VertexAdd(1L, src, Properties(), None)

    val data = kryo.serialise(message)

    graphProducer.sendAsync(data)

    messagesProcessed = messagesProcessed + 1

    if (messagesProcessed % 100_000 == 0)
      logger.info(s"Graph builder: Processed $messagesProcessed so far.")
  }

  private def messageListener(): MessageListener[Array[Byte]] =
    (consumer, msg) => {
      try {
        val data = kryo.deserialise[String](msg.getValue)

        processTuple(data)

        consumer.acknowledgeAsync(msg)
      }
      catch {
        case e: Exception =>
          logger.error("Failed to process data.")
          e.printStackTrace()

          consumer.negativeAcknowledge(msg)
      }
      finally msg.release()
    }
}

class Writer(pulsarController: PulsarController, topic: String) extends Runnable {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  controller.createListeningConsumer(
          subscriptionName = "writer",
          messageListener = messageListener(),
          schema = Schema.BYTES,
          topics = topic
  )

  def run(): Unit = {}

  private val kryo      = PulsarKryoSerialiser()
  var messagesProcessed = 0

  val storage = scala.collection.mutable.Map.empty[String, VertexAdd]

  def processMessage(data: GraphAlteration) =
    data match {
      case va: VertexAdd =>
        storage + (Random.nextString(5) -> va)

        messagesProcessed = messagesProcessed + 1

        if (messagesProcessed % 100_000 == 0)
          logger.info(s"Writer: Processed $messagesProcessed so far.")

      case _             =>
    }

  private def messageListener(): MessageListener[Array[Byte]] =
    (consumer, msg) => {
      try {
        val data = kryo.deserialise[GraphAlteration](msg.getValue)

        processMessage(data)

        consumer.acknowledgeAsync(msg)
      }
      catch {
        case e: Exception =>
          logger.error("Failed to process data.")
          e.printStackTrace()

          consumer.negativeAcknowledge(msg)
      }
      finally msg.release()
    }
}

class MonixScheduler {

  val threads: Int = 8

  // Will schedule things with delays
  val scheduledExecutor = Executors.newSingleThreadScheduledExecutor()

  // For actual execution of tasks
  val executorService =
    Scheduler.computation(parallelism = threads, executionModel = AlwaysAsyncExecution)

  // Logs errors to stderr or something
  val uncaughtExceptionReporter =
    UncaughtExceptionReporter(executorService.reportFailure)

  val scheduler = Scheduler(
          scheduledExecutor,
          executorService,
          uncaughtExceptionReporter,
          AlwaysAsyncExecution
  )
}
