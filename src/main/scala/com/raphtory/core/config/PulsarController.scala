package com.raphtory.core.config

import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api._
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class PulsarController(conf: Config) {
  private val pulsarAddress: String = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String    = conf.getString("raphtory.pulsar.admin.address")

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

  val consumerMaxMessages: Long  = conf.getLong("raphtory.consumers.maxMessagesForBatch")
  val consumerBatchTimeout: Long = conf.getLong("raphtory.consumers.timeoutForBatchMillis")
  val consumerPooling: Boolean   = conf.getBoolean("raphtory.consumers.consumerPoolPolicy")

  def setRetentionNamespace(
      namespace: String,
      retentionTime: Int = -1,
      retentionSize: Int = -1
  ): Unit = {
    val policies = new RetentionPolicies(retentionTime, retentionSize)
    pulsarAdmin.namespaces.setRetention(namespace, policies)
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
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .batchReceivePolicy(
              BatchReceivePolicy
                .builder()
                .maxNumMessages(1000)
                .timeout(100, TimeUnit.MICROSECONDS)
                .build()
      )
      .poolMessages(true)
      .messageListener(messageListener)
      .subscribe()

  // without message listener
  def createListeningConsumer[T](
      subscriptionName: String,
      schema: Schema[T],
      topics: String*
  ): Consumer[T] =
    client
      .newConsumer(schema)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .batchReceivePolicy(
              BatchReceivePolicy
                .builder()
                .maxNumMessages(100000)
                .timeout(1, TimeUnit.MILLISECONDS)
                .build()
      )
      .poolMessages(consumerPooling)
      .subscribe()

  def createConsumer[T](subscriptionName: String, schema: Schema[T], topics: String*): Consumer[T] =
    //topics.foreach(topic => pulsarAdmin.schemas().createSchema(topic, schema.getSchemaInfo))
    client
      .newConsumer(schema)
      .topics(topics.toList.asJava)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(10000).build())
      .poolMessages(true)
      .subscribe()

  def createProducer[T](schema: Schema[T], topic: String): Producer[T] =
    client
      .newProducer(schema)
      .topic(topic)
      .batchingMaxBytes(Int.MaxValue)
      .blockIfQueueFull(true)
      .create()
}
