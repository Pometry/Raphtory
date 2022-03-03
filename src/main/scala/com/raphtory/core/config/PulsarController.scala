package com.raphtory.core.config

import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api._
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy
import org.apache.pulsar.common.policies.data.BacklogQuota
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/** @DoNotDocument */
class PulsarController(conf: Config) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val pulsarAddress: String      = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String = conf.getString("raphtory.pulsar.admin.address")
  val spoutTopic: String         = conf.getString("raphtory.spout.topic")
  val deploymentID: String       = conf.getString("raphtory.deploy.id")
  val partitionServers: Int      = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int   = conf.getInt("raphtory.partitions.countPerServer")
  val hasDeletions: Boolean      = conf.getBoolean("raphtory.data.containsDeletions")
  val totalPartitions: Int       = partitionServers * partitionsPerServer

  private val numIoThreads         = conf.getInt("raphtory.pulsar.broker.ioThreads")
  private val connectionsPerBroker = conf.getInt("raphtory.pulsar.broker.connectionsPerBroker")

  private val useAllListenerThreads = "raphtory.pulsar.broker.useAvailableThreadsInSystem"

  private val listenerThreads =
    if (conf.hasPath(useAllListenerThreads) && conf.getBoolean(useAllListenerThreads))
      Runtime.getRuntime.availableProcessors()
    else
      conf.getInt("raphtory.pulsar.broker.listenerThreads")

  private val client: PulsarClient =
    PulsarClient
      .builder()
      .ioThreads(numIoThreads)
      .listenerThreads(listenerThreads)
      .connectionsPerBroker(connectionsPerBroker)
      .maxConcurrentLookupRequests(50_000)
      .maxLookupRequests(100_000)
      .serviceUrl(pulsarAddress)
      .serviceUrl(pulsarAdminAddress)
      .build()

  val pulsarAdmin: PulsarAdmin = PulsarAdmin.builder
    .serviceHttpUrl(pulsarAdminAddress)
    .tlsTrustCertsFilePath(null)
    .allowTlsInsecureConnection(false)
    .build

  def accessClient: PulsarClient = client

  def setRetentionNamespace(
      namespace: String,
      retentionTime: Int,
      retentionSize: Int
  ): Unit = {
    // TODO Re-enable and parameterise limitSize and limitTime
    //  Adding Backlog { } to conf and doing conf.hasPath
    //    pulsarAdmin
    //      .namespaces()
    //      .setBacklogQuota(
    //              namespace,
    //              BacklogQuota
    //                .builder()
    //                .limitSize(retentionSize)
    //                .limitTime(retentionTime)
    //                .retentionPolicy(RetentionPolicy.producer_exception)
    //                .build()
    //      )

    val policies = new RetentionPolicies(retentionTime, retentionSize)
    pulsarAdmin.namespaces.setRetention(namespace, policies)
    pulsarAdmin.namespaces().setDeduplicationStatus(namespace, false)
  }

  def createListeningConsumer[T](
      subscriptionName: String,
      messageListener: MessageListener[T],
      schema: Schema[T],
      topics: String*
  ): Consumer[T] =
    client
      .newConsumer(schema)
      .priorityLevel(0)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .maxTotalReceiverQueueSizeAcrossPartitions(Integer.MAX_VALUE)
      .receiverQueueSize(200_000)
      .poolMessages(true)
      .messageListener(messageListener)
      .subscribe()

  def createConsumer[T](subscriptionName: String, schema: Schema[T], topics: String*): Consumer[T] =
    client
      .newConsumer(schema)
      .priorityLevel(0)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .maxTotalReceiverQueueSizeAcrossPartitions(Integer.MAX_VALUE)
      .receiverQueueSize(200_000)
      .poolMessages(true)
      .subscribe()

  def createProducer[T](schema: Schema[T], topic: String): Producer[T] =
    client
      .newProducer(schema)
      .topic(topic)
      .enableBatching(true)
      .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
      .batchingMaxMessages(Integer.MAX_VALUE)
      .blockIfQueueFull(true)
      .sendTimeout(0, TimeUnit.MILLISECONDS)
      .maxPendingMessages(0)
      .create()

  def deleteTopic(topic: String) =
    pulsarAdmin.topics().delete(topic)

  def createTopic(component: String, topicSuffix: String): String = {
    val persistence = true

    val tenant        = conf.getString(s"raphtory.$component.tenant")
    val namespace     = conf.getString(s"raphtory.$component.namespace")
    val retentionTime = conf.getString(s"raphtory.$component.retentionTime").toInt
    val retentionSize = conf.getString(s"raphtory.$component.retentionSize").toInt

    try {
      pulsarAdmin.namespaces().createNamespace(s"$tenant/$namespace")
      setRetentionNamespace(
              namespace = s"$tenant/$namespace",
              retentionTime = retentionTime,
              retentionSize = retentionSize
      )
    }
    catch {
      case error: PulsarAdminException =>
        logger.debug(s"Namespace $namespace already exists.")
    }

    if (!persistence)
      s"non-persistent://$tenant/$namespace/$topicSuffix"
    else
      s"persistent://$tenant/$namespace/$topicSuffix"
  }

  // CREATION OF CONSUMERS
  def startGraphBuilderConsumer(
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {
    val topic = createTopic("spout", spoutTopic)
    createListeningConsumer("GraphBuilder", messageListener, Schema.BYTES, topic)
  }

  def startPartitionConsumer(
      partitionID: Int,
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {
    val topic1 = createTopic("builders", s"${deploymentID}_$partitionID")
    createListeningConsumer(
            s"Writer_$partitionID",
            messageListener,
            Schema.BYTES,
            topic1
    )
  }

  def startReaderConsumer(
      partitionID: Int,
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {
    val topic = createTopic("query", s"${deploymentID}_jobs")

    createListeningConsumer(
            s"Reader_$partitionID",
            messageListener,
            Schema.BYTES,
            topic
    )
  }

  def startQueryExecutorConsumer(
      partitionID: Int,
      jobID: String,
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {

    val topic = createTopic("query", s"${deploymentID}_${jobID}_$partitionID")
    createListeningConsumer(
            s"Executor_$partitionID",
            messageListener,
            Schema.BYTES,
            topic
    )
  }

  def startQueryManagerConsumer(
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {

    val topic = createTopic("query", s"${deploymentID}_submission")
    createListeningConsumer(
            "QueryManager",
            messageListener,
            Schema.BYTES,
            topic
    )
  }

  def startQueryHandlerConsumer(
      jobID: String,
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {
    val topic = createTopic("query", s"${deploymentID}_${jobID}_queryHandler")
    createListeningConsumer(
            s"QueryHandler_$jobID",
            messageListener,
            Schema.BYTES,
            topic
    )
  }

  def startQueryTrackerConsumer(
      jobId: String,
      messageListener: MessageListener[Array[Byte]]
  ): Consumer[Array[Byte]] = {

    val topic = createTopic("query", s"${deploymentID}_${jobId}_querytracking")
    createListeningConsumer(
            "queryProgressConsumer",
            messageListener,
            Schema.BYTES,
            topic
    )
  }

  // CREATION OF PRODUCERS

  def toBuildersProducer(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Builder producer.")

    val producerTopic = createTopic("spout", spoutTopic)
    createProducer(Schema.BYTES, producerTopic)
  }

  private def producerMapGenerator(topic: String): Map[Int, Producer[Array[Byte]]] = {
    val producerTopic = s"$topic"
    val producers     =
      for (i <- 0.until(totalPartitions))
        yield (i, createProducer(Schema.BYTES, producerTopic + s"_$i"))

    producers.toMap
  }

  def toWriterProducers: Map[Int, Producer[Array[Byte]]] = {
    val producerTopic = createTopic("builders", s"$deploymentID")

    producerMapGenerator(producerTopic)
  }

  def writerSyncProducers(): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating writer sync producer mapping.")

    val producerTopic = createTopic("builders", s"$deploymentID")
    producerMapGenerator(producerTopic)

  }

  def toReaderProducer: Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating Reader producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_jobs")
    createProducer(Schema.BYTES, producerTopic)
  }

  def toQueryExecutorProducers(jobID: String): Map[Int, Producer[Array[Byte]]] = {
    logger.debug(s"Deployment $deploymentID: Creating Query Executor producer mapping.")

    val producerTopic = createTopic("query", s"${deploymentID}_$jobID")
    producerMapGenerator(producerTopic)
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

    val producerTopic = createTopic("query", s"${deploymentID}_submission")
    createProducer(Schema.BYTES, producerTopic)

  }

  def globalwatermarkPublisher(): Producer[Array[Byte]] = {
    logger.debug(s"Deployment $deploymentID: Creating global watermark publisher producer.")

    val producerTopic = createTopic("query", s"${deploymentID}_watermark_global")
    createProducer(Schema.BYTES, producerTopic)

  }

}
