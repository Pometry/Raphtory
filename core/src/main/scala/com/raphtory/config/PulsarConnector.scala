package com.raphtory.config

import com.raphtory.components.Component
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class PulsarConnector(config: Config) extends Connector {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  case class PulsarEndPoint[T](producer: Producer[Array[Byte]]) extends EndPoint[T] {
    override def sendAsync(message: T): Unit = producer.sendAsync(serialise(message))
    override def close(): Unit               = producer.close()

    override def closeWithMessage(message: T): Unit =
      producer
        .flushAsync()
        .thenApply(_ => producer.send(serialise(message)))
        .thenApply(_ => producer.close())
  }

  val pulsarAddress: String         = config.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String    = config.getString("raphtory.pulsar.admin.address")
  private val numIoThreads          = config.getInt("raphtory.pulsar.broker.ioThreads")
  private val useAllListenerThreads = "raphtory.pulsar.broker.useAvailableThreadsInSystem"

  private val listenerThreads =
    if (config.hasPath(useAllListenerThreads) && config.getBoolean(useAllListenerThreads))
      Runtime.getRuntime.availableProcessors()
    else
      config.getInt("raphtory.pulsar.broker.listenerThreads")

  val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

  private val client: PulsarClient =
    PulsarClient
      .builder()
      .ioThreads(numIoThreads)
      .listenerThreads(listenerThreads)
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

  override def register[T](
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = {
    val consumerBuilders = topics
      .groupBy {
        case _: WorkPullTopic[T] => SubscriptionType.Shared
        case _                   => SubscriptionType.Exclusive
      }
      .view
      .mapValues(_ map createTopic)
      .map {
        case (subscriptionType, addresses) =>
          registerListener(messageHandler, addresses, subscriptionType)
      }
      .toSeq

    new CancelableListener {
      var consumers: Seq[Consumer[Array[Byte]]] = _
      override def start(): Unit                = consumers = consumerBuilders map (_.subscribe())
      override def close(): Unit                = consumers foreach (_.close())
    }
  }

  override def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T] = {
    val producerTopic = createTopic(topic)
    val producer      = createProducer(Schema.BYTES, producerTopic)
    PulsarEndPoint[T](producer)
  }

  private def registerListener[T](
      messageHandler: T => Unit,
      addresses: Seq[String],
      subscriptionType: SubscriptionType
  ): ConsumerBuilder[Array[Byte]] = {
    val messageListener: MessageListener[Array[Byte]] =
      (consumer, msg) => {
        try {
          val data: T = deserialise(msg.getValue)
          messageHandler.apply(data)
          consumer.acknowledgeAsync(msg)
        }
        catch {
          case e: Exception =>
            e.printStackTrace()
            consumer.negativeAcknowledge(msg)
            throw e
        }
        finally msg.release()
      }
    configuredConsumerBuilder(messageHandler.hashCode().toString, Schema.BYTES, addresses)
      .subscriptionType(subscriptionType)
      .messageListener(messageListener)
  }

  private def deserialise[T](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)
  private def serialise(value: Any): Array[Byte]    = kryo.serialise(value)

  private def configuredConsumerBuilder[T](
      subscriptionName: String,
      schema: Schema[T],
      topics: Seq[String] //TODO: cleanup this
  ): ConsumerBuilder[T] =
    client
      .newConsumer(schema)
      .priorityLevel(0)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .maxTotalReceiverQueueSizeAcrossPartitions(Integer.MAX_VALUE)
      .receiverQueueSize(200_000)
      .poolMessages(true)

  private def createTopic[T](topic: Topic[T]): String = {
    val persistence = true

//    val tenant        = config.getString(s"raphtory.$component.tenant") TODO: reenable this
//    val namespace     = config.getString(s"raphtory.$component.namespace")
//    val retentionTime = config.getString(s"raphtory.$component.retentionTime").toInt
//    val retentionSize = config.getString(s"raphtory.$component.retentionSize").toInt
    val tenant        = "public"
    val namespace     = config.getString("raphtory.deploy.id")
    val retentionTime = config.getString(s"raphtory.pulsar.retention.time").toInt
    val retentionSize = config.getString(s"raphtory.pulsar.retention.size").toInt

    try {
      pulsarAdmin.namespaces().createNamespace(s"$tenant/$namespace")
      setRetentionNamespace(
              namespace = s"$tenant/$namespace",
              retentionTime = retentionTime,
              retentionSize = retentionSize
      )
    }
    catch {
      case _: PulsarAdminException =>
        logger.debug(s"Namespace $namespace already exists.")
    }

    val subTopicSuffix = if (topic.subTopic.nonEmpty) s"-${topic.subTopic}" else ""
    val protocol       = if (!persistence) "non-persistent" else "persistent"

    s"$protocol://$tenant/$namespace/${topic.id}$subTopicSuffix"
  }

  private def createProducer[T](schema: Schema[T], topic: String): Producer[T] =
    client
      .newProducer(schema)
      .topic(topic)
      .enableBatching(true)
      .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
      .batchingMaxMessages(Int.MaxValue)
      .blockIfQueueFull(true)
      .maxPendingMessages(1000)
      .create()

  private def setRetentionNamespace(
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
}
