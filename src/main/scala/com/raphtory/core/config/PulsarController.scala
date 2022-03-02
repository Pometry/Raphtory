package com.raphtory.core.config

import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api._
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy
import org.apache.pulsar.common.policies.data.BacklogQuota
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/** @DoNotDocument */
class PulsarController(conf: Config) {
  private val pulsarAddress: String      = conf.getString("raphtory.pulsar.broker.address")
  private val pulsarAdminAddress: String = conf.getString("raphtory.pulsar.admin.address")

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
      retentionTime: Int = -1,
      retentionSize: Int = -1
  ): Unit = {
    pulsarAdmin
      .namespaces()
      .setBacklogQuota(
              namespace,
              BacklogQuota
                .builder()
                .limitSize(retentionSize)
                .limitTime(retentionTime)
                .retentionPolicy(RetentionPolicy.producer_exception)
                .build()
      )

    pulsarAdmin.namespaces().setDeduplicationStatus(namespace, false)
  }

  def setRetentionTopic(topic: String, retentionTime: Int = -1, retentionSize: Int = -1): Unit = {
    val policies = new RetentionPolicies(retentionTime, retentionSize)
    pulsarAdmin.topics.setRetention(topic, policies)
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
}
