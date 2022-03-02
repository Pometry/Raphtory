package com.raphtory.core.components

import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.serialisers.avro
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.policies.data.RetentionPolicies
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

  def createTopic(component: String, topicSuffix: String): String = {
    val persistence = conf.getBoolean(s"raphtory.$component.persistence")
    val tenant      = conf.getString(s"raphtory.$component.tenant")
    val namespace   = conf.getString(s"raphtory.$component.namespace")
    if (!persistence) {
      pulsarController.setupComponentNamespace(s"$tenant/$namespace")
      s"non-persistent://$tenant/$namespace/$topicSuffix"
    }
    else {
      pulsarController.setupComponentNamespace(s"$tenant/$namespace")
      s"persistent://$tenant/$namespace/$topicSuffix"
    }
  }

  // CREATION OF CONSUMERS
  def startGraphBuilderConsumer(schema: Schema[T]): Consumer[T] = {
    val topic = createTopic("spout", spoutTopic)
    pulsarController.createListeningConsumer("GraphBuilder", messageListener, schema, topic)
  }

  def startPartitionConsumer(schema: Schema[T], partitionID: Int): Consumer[T] = {
    val topic1 = createTopic("builders", s"${deploymentID}_$partitionID")
    val topic2 = createTopic("builders", s"${deploymentID}_sync_$partitionID")
    pulsarController.createListeningConsumer(
            s"Writer_$partitionID",
            messageListener,
            schema,
            topic1,
            topic2
    )
  }

  def startReaderConsumer(schema: Schema[T], partitionID: Int): Consumer[T] = {
    val topic = createTopic("query", s"${deploymentID}_jobs")

    pulsarController.createListeningConsumer(
            s"Reader_$partitionID",
            messageListener,
            schema,
            topic.toString
    )
  }

  def startQueryExecutorConsumer(
      schema: Schema[T],
      partitionID: Int,
      jobID: String
  ): Consumer[T] = {

    val topic = createTopic("query", s"${deploymentID}_${jobID}_$partitionID")
    pulsarController.createListeningConsumer(
            s"Executor_$partitionID",
            messageListener,
            schema,
            topic.toString
    )
  }

  def startQueryManagerConsumer(schema: Schema[T]): Consumer[T] = {

    val topic1 = createTopic("query", s"${deploymentID}_watermark")
    val topic2 = createTopic("query", s"${deploymentID}_submission")
    pulsarController.createListeningConsumer(
            "QueryManager",
            messageListener,
            schema,
            topic1.toString,
            topic2.toString
    )
  }

  def startQueryHandlerConsumer(schema: Schema[T], jobID: String): Consumer[T] = {
    val topic = createTopic("query", s"${deploymentID}_${jobID}_queryHandler")
    pulsarController.createListeningConsumer(
            s"QueryHandler_$jobID",
            messageListener,
            schema,
            topic.toString
    )
  }

  def startQueryTrackerConsumer(schema: Schema[T], deployId_jobId: String): Consumer[T] = {

    val topic = createTopic("query", s"${deployId_jobId}_querytracking")
    pulsarController.createListeningConsumer(
            "queryProgressConsumer",
            messageListener,
            schema,
            topic
    )
  }

  // CREATION OF PRODUCERS

  def toBuildersProducer[T](schema: Schema[T]): Producer[T] = {
    logger.debug(s"Deployment $deploymentID: Creating Builder producer.")

    val producerTopic = createTopic("spout", spoutTopic)
    pulsarController.createProducer(schema, producerTopic)
  }

  private def producerMapGenerator[T](topic: String, schema: Schema[T]): Map[Int, Producer[T]] = {
    //createTopic[T](s"${deploymentID}_$i", schema)
    val producerTopic = s"$topic"
    val producers     =
      for (i <- 0.until(totalPartitions))
        yield (i, pulsarController.createProducer(schema, producerTopic.toString + s"_$i"))

    producers.toMap
  }

  def toWriterProducers: Map[Int, Producer[GraphAlteration]] = {
    implicit val schema: Schema[GraphAlteration] = GraphAlteration.schema

    val producerTopic = createTopic("builders", s"$deploymentID")
    producerMapGenerator[GraphAlteration](producerTopic, schema)
  }

  def writerSyncProducers(): Map[Int, Producer[GraphAlteration]] = {
    logger.debug(s"Deployment $deploymentID: Creating writer sync producer mapping.")
    implicit val schema: Schema[GraphAlteration] = GraphAlteration.schema

    val producerTopic = createTopic("builders", s"${deploymentID}_sync")
    producerMapGenerator(producerTopic, schema)

  }

  def toReaderProducer: Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Reader producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_jobs")
    pulsarController.createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryExecutorProducers(jobID: String): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Executor producer mapping.")
    val producerTopic = createTopic("query", s"${deploymentID}_$jobID")
    producerMapGenerator(producerTopic, Schema.BYTES)
  }

  def toQueryManagerProducer: Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Manager producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_submission")
    pulsarController.createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryHandlerProducer(jobID: String): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Handler producer for job '$jobID'.")

    val producerTopic = createTopic("query", s"${deploymentID}_${jobID}_queryHandler")
    pulsarController.createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryTrackerProducer(jobID: String): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Tracker producer for job '$jobID'.")

    val producerTopic = createTopic("query", s"${deploymentID}_${jobID}_querytracking")
    pulsarController.createProducer(Schema.BYTES, producerTopic)
  }

  def watermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Watermark Publisher producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_watermark")
    pulsarController.createProducer(Schema.BYTES, producerTopic)

  }

  def globalwatermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating global watermark publisher producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_watermark_global")
    pulsarController.createProducer(Schema.BYTES, producerTopic)

  }

  def createNamespace(namespace: String): Unit =
    pulsarController.createNamespace(namespace)
}
