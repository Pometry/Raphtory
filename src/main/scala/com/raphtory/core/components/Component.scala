package com.raphtory.core.components

import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.serialisers.avro
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.collection.parallel.mutable.ParArray
import scala.reflect.runtime.universe._

abstract class Component[SENDING, RECEIVING](
    conf: Config,
    private val pulsarController: PulsarController,
    scheduler: Scheduler
) extends Runnable {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val pulsarAddress: String                   = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String              = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String                      = conf.getString("raphtory.spout.topic")
  val deploymentID: String                    = conf.getString("raphtory.deploy.id")
  val partitionServers: Int                   = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int                = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean                   = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int                    = partitionServers * partitionsPerServer
  private val kryo: PulsarKryoSerialiser      = PulsarKryoSerialiser()
  val consumer: Option[Consumer[Array[Byte]]] = None

  def handleMessage(msg: RECEIVING): Boolean
  def run()
  def stop()
  def name(): String

  def getScheduler(): Scheduler = scheduler

//  def serialise(value: Any): Array[Byte] = kryo.serialise(value)
//
//  def deserialise[T: TypeTag](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)

  def sendSyncMessage(producer: Producer[Array[Byte]], msg: SENDING) =
    producer.send(kryo.serialise(msg))

  def sendMessage(producer: Producer[Array[Byte]], msg: SENDING) =
    Task(sendAsync(producer, msg)).runAsync {
      case Right(value) =>
      case Left(ex)     =>
        ex.printStackTrace()
    }(scheduler)

  private def sendAsync(producer: Producer[Array[Byte]], msg: SENDING) =
    producer.sendAsync(kryo.serialise(msg))

  def sendBatch(producer: Producer[Array[Byte]], msgs: Array[SENDING]) =
    Task(sendBatchAsync(producer, msgs)).runAsync {
      case Right(value) =>
      case Left(ex)     =>
        logger.error(s"ERROR: ${ex.getMessage}")
    }(scheduler)

  private def sendBatchAsync(producer: Producer[Array[Byte]], msgs: Array[SENDING]) =
    // val innerSerialised = msgs.map(msg => kryo.serialise(msg)) //TODO work out if we can do this
    producer.send(kryo.serialise(msgs))

  def getWriter(srcId: Long): Int = (srcId.abs % totalPartitions).toInt

  // CREATION OF CONSUMERS
  def startGraphBuilderConsumer(): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer("GraphBuilder", Schema.BYTES, spoutTopic)

  def startPartitionConsumer(partitionID: Int): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"Writer_$partitionID",
            Schema.BYTES,
            s"${deploymentID}_$partitionID",
            s"${deploymentID}_sync_$partitionID"
    )

  def startReaderConsumer(partitionID: Int): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"Reader_$partitionID",
            Schema.BYTES,
            s"${deploymentID}_jobs"
    )

  def startQueryExecutorConsumer(partitionID: Int, jobID: String): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"Executor_$partitionID",
            Schema.BYTES,
            s"${deploymentID}_${jobID}_$partitionID"
    )

  def startQueryManagerConsumer: Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            "QueryManager",
            Schema.BYTES,
            s"${deploymentID}_watermark",
            s"${deploymentID}_submission"
    )

  def startQueryHandlerConsumer(jobID: String): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            s"QueryHandler_$jobID",
            Schema.BYTES,
            s"${deploymentID}_${jobID}_queryHandler"
    )

  def startQueryTrackerConsumer(deployId_jobId: String): Consumer[Array[Byte]] =
    pulsarController.createListeningConsumer(
            "queryProgressConsumer",
            Schema.BYTES,
            s"${deployId_jobId}_querytracking"
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
