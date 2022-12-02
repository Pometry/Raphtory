package com.raphtory.pulsar.connector

import java.io.Closeable
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
) extends Closeable {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def accessClient: PulsarClient = client
  def adminClient: PulsarAdmin   = pulsarAdmin

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

    val listenerThreads =
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
