package com.raphtory.pulsar.connector

import java.io.Closeable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import cats.effect.Async
import cats.effect.kernel.Resource
import com.raphtory.internals.communication._
import com.raphtory.internals.serialisers.KryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.PulsarClientException.BrokerMetadataException
import org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException
import org.apache.pulsar.client.api._
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.NonFatal

private[raphtory] class PulsarConnector(
    client: PulsarClient,
    pulsarAdmin: PulsarAdmin,
    config: Config
) extends Connector
        with Closeable {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  case class PulsarEndPoint[T](producer: Producer[Array[Byte]]) extends EndPoint[T] {

    override def sendAsync(message: T): Unit = {
      logger.trace(s"sending message: '$message' to topic: '${producer.getTopic}'")
      try producer.sendAsync(serialise(message))
      catch {
        case NonFatal(t) =>
          logger.error(s"Failed to send MSG $message", t)
          throw t
      }
    }

    override def sendSync(message: T): Unit =
      flushAndSendAsync(message).get()

    override def close(): Unit = producer.flushAsync().thenApply(_ => producer.closeAsync())

    override def flushAsync(): CompletableFuture[Void] = producer.flushAsync()

    override def closeWithMessage(message: T) =
      producer
        .flushAsync()
        .thenApply(_ => producer.sendAsync(serialise(message)).thenApply(_ => producer.closeAsync()))
        .get()
  }

  private val kryo: KryoSerialiser = KryoSerialiser()

  def accessClient: PulsarClient = client

  def adminClient: PulsarAdmin = pulsarAdmin

  override def register[T](
      id: String,
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
          logger.debug(s"registering pulsar listener for address $addresses")
          registerListener(id, messageHandler, addresses, subscriptionType)
      }
      .toSeq

    new CancelableListener {
      var consumers: Seq[Consumer[Array[Byte]]] = Seq()
      override def start(): Unit                =
        try {
          consumers = consumerBuilders map (_.subscribe())
          logger.debug(s"consumer created for topics $topics")
        }
        catch {
          case e: ConsumerBusyException =>
            val msg = s"Impossible to create consumer for some of these topics: $topics"
            logger.error(msg)
            throw new IllegalStateException(msg, e)
        }
      override def close(): Unit                =
        consumers foreach { consumer =>
          try consumer.unsubscribe()
          catch {
            case e: BrokerMetadataException =>
              logger.info(
                      s"Consumer ${consumer.getConsumerName} failed to unsubscribe from ${consumer.getTopic} during close. Attempting close now."
              )
          }
          consumer.close()
        }
    }
  }

  override def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T] = {
    val producerTopic = createTopic(topic)
    logger.debug(s"creating pulsar producer for $producerTopic")
    val producer      = createProducer(Schema.BYTES, producerTopic)
    PulsarEndPoint[T](producer)
  }

  private def registerListener[T](
      listenerId: String,
      messageHandler: T => Unit,
      addresses: Seq[String],
      subscriptionType: SubscriptionType
  ): ConsumerBuilder[Array[Byte]] = {

    val messageListener: MessageListener[Array[Byte]] = new MessageListener[Array[Byte]] {
      val lastIds: mutable.Map[String, MessageId]                                             = mutable.Map()
      override def received(consumer: Consumer[Array[Byte]], msg: Message[Array[Byte]]): Unit =
        try {
          val data   = deserialise[T](msg.getValue)
          val id     = msg.getMessageId
          val lastId = lastIds.getOrElse(msg.getTopicName, MessageId.earliest)
          if (id <= lastId)
            logger.error(
                    s"Message $data received by $listenerId with index $id <= last index $lastId. Ignoring message"
            )
          else {
            lastIds(msg.getTopicName) = id
            messageHandler.apply(data)
          }
          consumer.acknowledgeAsync(msg)
        }
        catch {
          case e: Exception =>
            logger.error(s"Component $listenerId: Failed to handle message. ${e.getMessage}", e)
            consumer.negativeAcknowledge(msg)
            throw e
        }
        finally msg.release()
    }
    val subscriptionName                              = subscriptionType match {
      case SubscriptionType.Exclusive  => listenerId
      case SubscriptionType.Shared     => "shared"
      case SubscriptionType.Failover   => listenerId // not used
      case SubscriptionType.Key_Shared => listenerId // not used
    }
    configuredConsumerBuilder(subscriptionName, Schema.BYTES, addresses)
      .subscriptionType(subscriptionType)
      .messageListener(messageListener)
  }

  private def deserialise[T](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)
  private def serialise(value: Any): Array[Byte]    = kryo.serialise(value)

  private def configuredConsumerBuilder[T](
      subscriptionName: String,
      schema: Schema[T],
      topics: Seq[String]
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

  def createSharedConsumer[T](
      subscriptionName: String,
      schema: Schema[T],
      topics: String*
  ): Consumer[T] =
    configuredConsumerBuilder(subscriptionName, schema, topics)
      .subscriptionType(SubscriptionType.Shared)
      .subscribe()

  def createExclusiveConsumer[T](
      subscriptionName: String,
      schema: Schema[T],
      topics: String*
  ): Consumer[T] =
    configuredConsumerBuilder(subscriptionName, schema, topics)
      .subscriptionType(SubscriptionType.Exclusive)
      .subscribe()

  private def createTopic[T](topic: Topic[T]): String = {
    val persistence   = true
    val tenant        = config.getString(s"raphtory.pulsar.topics.${topic.id}.tenant")
    val namespace     = config.getString(s"raphtory.pulsar.topics.${topic.id}.namespace")
    val retentionTime = config.getString(s"raphtory.pulsar.topics.${topic.id}.retention.time").toInt
    val retentionSize = config.getString(s"raphtory.pulsar.topics.${topic.id}.retention.size").toInt

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
        logger.trace(s"Namespace $namespace already exists.")
    }

    val protocol = if (!persistence) "non-persistent" else "persistent"
    val address  = if (topic.customAddress.isEmpty) {
      val subTopicSuffix = if (topic.subTopic.nonEmpty) s"-${topic.subTopic}" else ""
      s"${topic.id}$subTopicSuffix"
    }
    else
      topic.customAddress

    s"$protocol://$tenant/$namespace/$address"
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

  override def close(): Unit = {
    if (!client.isClosed())
      client.close()

    pulsarAdmin.close()
  }

}

object PulsarConnector {

  def unsafeApply(config: Config): PulsarConnector = {
    val client      = makePulsarClient(config)
    val adminClient = makeAdminClient(config)
    new PulsarConnector(client, adminClient, config)
  }

  def apply[IO[_]](config: Config)(implicit IO: Async[IO]): Resource[IO, PulsarConnector] =
    for {
      pulsarClient      <- Resource.fromAutoCloseable(IO.blocking(makePulsarClient(config)))
      pulsarAdminClient <- Resource.fromAutoCloseable(IO.blocking(makeAdminClient(config)))
    } yield new PulsarConnector(pulsarClient, pulsarAdminClient, config)

  private def makePulsarClient(config: Config): PulsarClient = {

    val numIoThreads          = config.getInt("raphtory.pulsar.broker.ioThreads")
    val pulsarAddress: String = config.getString("raphtory.pulsar.broker.address")

    val useAllListenerThreads = "raphtory.pulsar.broker.useAvailableThreadsInSystem"

    val pulsarAdminAddress: String = config.getString("raphtory.pulsar.admin.address")
    val listenerThreads            =
      if (config.hasPath(useAllListenerThreads) && config.getBoolean(useAllListenerThreads))
        Runtime.getRuntime.availableProcessors()
      else
        config.getInt("raphtory.pulsar.broker.listenerThreads")

    val client: PulsarClient =
      PulsarClient
        .builder()
        .ioThreads(numIoThreads)
        .listenerThreads(listenerThreads)
        .maxConcurrentLookupRequests(50_000)
        .maxLookupRequests(100_000)
        .serviceUrl(pulsarAddress)
        .build()
    client
  }

  private def makeAdminClient(config: Config): PulsarAdmin = {
    val pulsarAdminAddress: String = config.getString("raphtory.pulsar.admin.address")
    PulsarAdmin.builder
      .serviceHttpUrl(pulsarAdminAddress)
      .tlsTrustCertsFilePath(null)
      .allowTlsInsecureConnection(false)
      .build
  }
}
