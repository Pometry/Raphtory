package com.raphtory.config

import com.raphtory.components.Component
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.MessageListener
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.api.SubscriptionType

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class PulsarConnector(config: Config) extends Connector {

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

  override def registerExclusiveListener[T](component: Component[T], address: String): Unit = {
    val consumerBuilder = configuredConsumerBuilder(address, Schema.BYTES, Seq(address))
      .subscriptionType(SubscriptionType.Exclusive)
    registerListener[T](component, consumerBuilder)
  }

  override def registerBroadcastListener[T](
      component: Component[T],
      address: String,
      partition: Int
  ): Unit = {
    val consumerBuilder =
      configuredConsumerBuilder(s"$address-$partition", Schema.BYTES, Seq(address))
        .subscriptionType(SubscriptionType.Exclusive)
    registerListener(component, consumerBuilder)
  }

  override def registerWorkPullListener[T](component: Component[T], address: String): Unit = {
    val consumerBuilder = configuredConsumerBuilder(address, Schema.BYTES, Seq(address))
      .subscriptionType(SubscriptionType.Shared)
    registerListener(component, consumerBuilder)
  }

  override def createExclusiveEndPoint[T](address: String): EndPoint[T] = createEndPoint(address)
  override def createWorkPullEndPoint[T](address: String): EndPoint[T]  = createEndPoint(address)
  override def createBroadcastEndPoint[T](address: String): EndPoint[T] = createEndPoint(address)

  private def registerListener[T](
      component: Component[T],
      consumerBuilder: ConsumerBuilder[Array[Byte]]
  ): Unit = {
    val messageListener: MessageListener[Array[Byte]] =
      (consumer, msg) => {
        try {
          val data: T = deserialise(msg.getValue)
          component.handleMessage(data)
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
    component.listeningSetup.add { () =>
      val consumer = consumerBuilder.messageListener(messageListener).subscribe()
      component.listeningCleanup.add(consumer.close)
    }
  }

  private def deserialise[T](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)
  private def serialise(value: Any): Array[Byte]    = kryo.serialise(value)

  private def createEndPoint[T](topic: String): EndPoint[T] = {
    val producer: Producer[Array[Byte]] = createProducer(Schema.BYTES, topic)
    PulsarEndPoint[T](producer)
  }

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
}
