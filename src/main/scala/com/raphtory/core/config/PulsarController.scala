package com.raphtory.core.config

import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.admin.Namespaces
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.admin.Topics
import org.apache.pulsar.client.api._
import org.apache.pulsar.common.policies.data.PersistencePolicies
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class PulsarController(conf: Config) {
  val retentionTimeout: Int      = conf.getString("raphtory.pulsar.retention.time").toInt
  val retentionSize: Int         = conf.getString("raphtory.pulsar.retention.size").toInt
  val pulsarAddress: String      = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String         = conf.getString("raphtory.spout.topic")
  val deploymentID: String       = conf.getString("raphtory.deploy.id")
  val partitionServers: Int      = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int   = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean      = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int       = partitionServers * partitionsPerServer

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val client: PulsarClient =
    PulsarClient
      .builder()
      .ioThreads(10)
      .serviceUrl(pulsarAddress)
      .serviceUrl(pulsarAdminAddress)
      .build()

  val pulsarAdmin = PulsarAdmin.builder
    .serviceHttpUrl(pulsarAdminAddress)
    .tlsTrustCertsFilePath(null)
    .allowTlsInsecureConnection(false)
    .build

  def accessClient: PulsarClient = client

  def setRetentionNamespace(
      namespace: String,
      retentionTime: Int = retentionTimeout,
      retentionSize: Int = retentionSize
  ): Unit = {
    val policies = new RetentionPolicies(retentionTime, retentionSize)
    pulsarAdmin.namespaces.setRetention(namespace, policies)
  }

  def readCompact(topicList: List[String]) : Boolean = {
    val non_persistent = "non-persistent"
    topicList.exists(non_persistent.contains(_))
  }

  def createListeningConsumer[T](
      subscriptionName: String,
      messageListener: MessageListener[T],
      schema: Schema[T],
      topics: String*
  ): Consumer[T] = {
    val isCompact = readCompact(topics.toList)
    client
      .newConsumer(schema)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .batchReceivePolicy(
              BatchReceivePolicy
                .builder()
                .maxNumMessages(10000)
                .timeout(1, TimeUnit.NANOSECONDS)
                .build()
      )
      .poolMessages(true)
      .messageListener(messageListener)
      .readCompacted(isCompact)
      .subscribe()
  }

  def createConsumer[T](subscriptionName: String, schema: Schema[T], topics: String*): Consumer[T] = {
    //topics.foreach(topic => pulsarAdmin.schemas().createSchema(topic, schema.getSchemaInfo))

    val isCompact = readCompact(topics.toList)
    client
      .newConsumer(schema)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(10000).build())
      .poolMessages(true)
      .readCompacted(isCompact)
      .subscribe()
  }

  def createProducer[T](schema: Schema[T], topic: String): Producer[T] =
    client.newProducer(schema).topic(topic).blockIfQueueFull(true).create() //.enableBatching(true)

  def deleteTopic(topic: String) =
    pulsarAdmin.topics().delete(topic)

  def createNamespace(namespace: String): Boolean =
    try {
      pulsarAdmin.namespaces().createNamespace(namespace)
      true
    }
    catch {
      case error: PulsarAdminException =>
        false
    }

  def setupComponentNamespace(namespace: String): Unit = {
    val setRetention = createNamespace(namespace)
    if (setRetention) setRetentionNamespace(namespace)
  }

  def createTopic(component: String, topicSuffix: String): String = {
    val persistence = true //conf.getBoolean(s"raphtory.$component.persistence")
    val tenant      = conf.getString(s"raphtory.$component.tenant")
    val namespace   = conf.getString(s"raphtory.$component.namespace")
    if (!persistence) {
      setupComponentNamespace(s"$tenant/$namespace")
      s"non-persistent://$tenant/$namespace/$topicSuffix"
    }
    else {
      setupComponentNamespace(s"$tenant/$namespace")
      s"persistent://$tenant/$namespace/$topicSuffix"
    }
  }

  // CREATION OF CONSUMERS
  def startGraphBuilderConsumer[T](
      schema: Schema[T],
      messageListener: MessageListener[T]
  ): Consumer[T] = {
    val topic = createTopic("spout", spoutTopic)
    createListeningConsumer("GraphBuilder", messageListener, schema, topic)
  }

  def startPartitionConsumer[T](
      schema: Schema[T],
      partitionID: Int,
      messageListener: MessageListener[T]
  ): Consumer[T] = {
    val topic1 = createTopic("builders", s"${deploymentID}_$partitionID")
    val topic2 = createTopic("builders", s"${deploymentID}_sync_$partitionID")
    createListeningConsumer(
            s"Writer_$partitionID",
            messageListener,
            schema,
            topic1,
            topic2
    )
  }

  def startReaderConsumer[T](
      schema: Schema[T],
      partitionID: Int,
      messageListener: MessageListener[T]
  ): Consumer[T] = {
    val topic = createTopic("query", s"${deploymentID}_jobs")

    createListeningConsumer(
            s"Reader_$partitionID",
            messageListener,
            schema,
            topic.toString
    )
  }

  def startQueryExecutorConsumer[T](
      schema: Schema[T],
      partitionID: Int,
      jobID: String,
      messageListener: MessageListener[T]
  ): Consumer[T] = {

    val topic = createTopic("query", s"${deploymentID}_${jobID}_$partitionID")
    createListeningConsumer(
            s"Executor_$partitionID",
            messageListener,
            schema,
            topic.toString
    )
  }

  def startQueryManagerConsumer[T](
      schema: Schema[T],
      messageListener: MessageListener[T]
  ): Consumer[T] = {

    val topic1 = createTopic("query", s"${deploymentID}_watermark")
    val topic2 = createTopic("query", s"${deploymentID}_submission")
    createListeningConsumer(
            "QueryManager",
            messageListener,
            schema,
            topic1.toString,
            topic2.toString
    )
  }

  def startQueryHandlerConsumer[T](
      schema: Schema[T],
      jobID: String,
      messageListener: MessageListener[T]
  ): Consumer[T] = {
    val topic = createTopic("query", s"${deploymentID}_${jobID}_queryHandler")
    createListeningConsumer(
            s"QueryHandler_$jobID",
            messageListener,
            schema,
            topic.toString
    )
  }

  def startQueryTrackerConsumer[T](
      schema: Schema[T],
      jobId: String,
      messageListener: MessageListener[T]
  ): Consumer[T] = {

    val topic = createTopic("query", s"${deploymentID}_${jobId}_querytracking")
    createListeningConsumer(
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
    createProducer(schema, producerTopic)
  }

  private def producerMapGenerator[T](topic: String, schema: Schema[T]): Map[Int, Producer[T]] = {
    //createTopic[T](s"${deploymentID}_$i", schema)
    val producerTopic = s"$topic"
    val producers     =
      for (i <- 0.until(totalPartitions))
        yield (i, createProducer(schema, producerTopic.toString + s"_$i"))

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
    createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryExecutorProducers(jobID: String): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Executor producer mapping.")
    val producerTopic = createTopic("query", s"${deploymentID}_$jobID")
    producerMapGenerator(producerTopic, Schema.BYTES)
  }

  def toQueryManagerProducer: Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Manager producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_submission")
    createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryHandlerProducer(jobID: String): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Handler producer for job '$jobID'.")

    val producerTopic = createTopic("query", s"${deploymentID}_${jobID}_queryHandler")
    createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryTrackerProducer(jobID: String): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Tracker producer for job '$jobID'.")

    val producerTopic = createTopic("query", s"${deploymentID}_${jobID}_querytracking")
    createProducer(Schema.BYTES, producerTopic)
  }

  def watermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Watermark Publisher producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_watermark")
    createProducer(Schema.BYTES, producerTopic)

  }

  def globalwatermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating global watermark publisher producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_watermark_global")
    createProducer(Schema.BYTES, producerTopic)

  }

}
