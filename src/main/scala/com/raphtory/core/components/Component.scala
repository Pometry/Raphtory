package com.raphtory.core.components

import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._

abstract class Component[T](conf: Config, private val pulsarController: PulsarController)
        extends Runnable {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val pulsarAddress: String              = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String         = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String                 = conf.getString("raphtory.spout.topic")
  val deploymentID: String               = conf.getString("raphtory.deploy.id")
  val partitionServers: Int              = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int           = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean              = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int               = partitionServers * partitionsPerServer
  private val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

  def handleMessage(msg: Message[T])
  def run()
  def stop()

  private def messageListener(): MessageListener[T] =
    (consumer, msg) => {
      try {
        handleMessage(msg)
        consumer.acknowledge(msg)
      }
      catch {
        case e: Exception =>
          logger.error(s"Deployment $deploymentID: Failed to handle message.")
          e.printStackTrace()

          consumer.negativeAcknowledge(msg)
      }
    }

  def serialise(value: Any): Array[Byte] = kryo.serialise(value)

  def deserialise[T: TypeTag](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt

  // CREATION OF CONSUMERS
  def startGraphBuilderConsumer(schema: Schema[T]): Consumer[T] =
    pulsarController.createListeningConsumer("GraphBuilder", messageListener, schema, spoutTopic)

  def startPartitionConsumer(schema: Schema[T], partitionID: Int): Consumer[T] =
    pulsarController.createListeningConsumer(
            s"Writer_$partitionID",
            messageListener,
            schema,
            s"${deploymentID}_$partitionID",
            s"${deploymentID}_sync_$partitionID"
    )

  def startReaderConsumer(schema: Schema[T], partitionID: Int): Consumer[T] =
    pulsarController.createListeningConsumer(
            s"Reader_$partitionID",
            messageListener,
            schema,
            s"${deploymentID}_jobs"
    )

  def startQueryExecutorConsumer(schema: Schema[T], partitionID: Int, jobID: String): Consumer[T] =
    pulsarController.createListeningConsumer(
            s"Executor_$partitionID",
            messageListener,
            schema,
            s"${deploymentID}_${jobID}_$partitionID"
    )

  def startQueryManagerConsumer(schema: Schema[T]): Consumer[T] =
    pulsarController.createListeningConsumer(
            "QueryManager",
            messageListener,
            schema,
            s"${deploymentID}_watermark",
            s"${deploymentID}_submission"
    )

  def startQueryHandlerConsumer(schema: Schema[T], jobID: String): Consumer[T] =
    pulsarController.createListeningConsumer(
            s"QueryHandler_$jobID",
            messageListener,
            schema,
            s"${deploymentID}_${jobID}_queryHandler"
    )

  def startQueryTrackerConsumer(schema: Schema[T], deployId_jobId: String): Consumer[T] =
    pulsarController.createListeningConsumer(
            "queryProgressConsumer",
            messageListener,
            schema,
            s"${deployId_jobId}_querytracking"
    )

  // CREATION OF PRODUCERS
  private def producerMapGenerator(topic: String): Map[Int, Producer[Array[Byte]]] = {
    val producers =
      for (i <- 0.until(totalPartitions))
        yield (i, pulsarController.createProducer(Schema.BYTES, topic + s"_$i"))
    producers.toMap
  }

  def toWriterProducers: Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating producer of topic '$deploymentID' mapping")

    producerMapGenerator(deploymentID)
  }

  def writerSyncProducers(): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating writer sync producer mapping.")

    producerMapGenerator(s"${deploymentID}_sync")
  }

  def toReaderProducer: Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Reader producer.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_jobs")
  }

  def toQueryExecutorProducers(jobID: String): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Executor producer mapping.")

    producerMapGenerator(s"${deploymentID}_$jobID")
  }

  def toQueryManagerProducer: Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Manager producer.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_submission")
  }

  def toQueryHandlerProducer(jobID: String): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Handler producer for job '$jobID'.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_${jobID}_queryHandler")
  }

  def toQueryTrackerProducer(jobID: String): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Tracker producer for job '$jobID'.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_${jobID}_querytracking")
  }

  def watermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Watermark Publisher producer.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_watermark")
  }

  def globalwatermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating global watermark publisher producer.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_watermark_global")
  }

}
