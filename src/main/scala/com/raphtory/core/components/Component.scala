package com.raphtory.core.components

import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.serialisers.avro
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._

/** @DoNotDocument */
abstract class Component[T: TypeTag](conf: Config, private val pulsarController: PulsarController)
        extends Runnable {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val pulsarAddress: String      = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String         = conf.getString("raphtory.spout.topic")
  val deploymentID: String       = conf.getString("raphtory.deploy.id")
  val partitionServers: Int      = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int   = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean      = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int       = partitionServers * partitionsPerServer
  val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

  def handleMessage(msg: T)
  def run()
  def stop()

  private def messageListener(): MessageListener[Array[Byte]] =
    (consumer, msg) => {
      try {
        val data = deserialise[T](msg.getValue)

        handleMessage(data)

        consumer.acknowledgeAsync(msg)
      }
      catch {
        case e: Exception =>
          logger.error(s"Deployment $deploymentID: Failed to handle message.")
          e.printStackTrace()

          consumer.negativeAcknowledge(msg)
      }
      finally msg.release()
    }

  def serialise(value: Any): Array[Byte] = kryo.serialise(value)

  def deserialise[T: TypeTag](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt

  // CREATION OF CONSUMERS
  def startGraphBuilderConsumer(): Consumer[Array[Byte]] =
    pulsarController
      .createListeningConsumer("GraphBuilder", messageListener, Schema.BYTES, spoutTopic)

  def startPartitionConsumer(partitionID: Int): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"Writer_$partitionID",
            messageListener,
            Schema.BYTES,
            s"${deploymentID}_$partitionID"
    )

  def startReaderConsumer(partitionID: Int): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"Reader_$partitionID",
            messageListener,
            Schema.BYTES,
            s"${deploymentID}_jobs"
    )

  def startQueryExecutorConsumer(partitionID: Int, jobID: String): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"Executor_$partitionID",
            messageListener,
            Schema.BYTES,
            s"${deploymentID}_${jobID}_$partitionID"
    )

  def startQueryManagerConsumer(): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            "QueryManager",
            messageListener,
            Schema.BYTES,
            s"${deploymentID}_submission"
    )

  def startQueryHandlerConsumer(jobID: String): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            subscriptionName = s"QueryHandler_$jobID",
            messageListener = messageListener(),
            schema = Schema.BYTES,
            topics = s"${deploymentID}_${jobID}_queryHandler"
    )

  def startQueryTrackerConsumer(deployId_jobId: String): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            subscriptionName = "queryProgressConsumer",
            messageListener = messageListener(),
            schema = Schema.BYTES,
            topics = s"${deployId_jobId}_querytracking"
    )

  // CREATION OF PRODUCERS
  private def producerMapGenerator[T](topic: String): Map[Int, Producer[Array[Byte]]] = {
    //createTopic[T](s"${deploymentID}_$i", schema)
    val producers =
      for (i <- 0.until(totalPartitions))
        yield (i, pulsarController.createProducer(Schema.BYTES, topic + s"_$i"))

    producers.toMap
  }

  def toWriterProducers: Map[Int, Producer[Array[Byte]]] =
    //println(schema.getSchemaInfo.getSchemaDefinition)
    producerMapGenerator[Array[Byte]](deploymentID)

  def writerSyncProducers(): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating writer sync producer mapping.")

    producerMapGenerator(s"$deploymentID")
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

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_submission")
  }

  def globalwatermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating global watermark publisher producer.")

    pulsarController.createProducer(Schema.BYTES, s"${deploymentID}_watermark_global")
  }
}
