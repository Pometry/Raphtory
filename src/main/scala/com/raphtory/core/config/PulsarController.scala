package com.raphtory.core.config

import com.typesafe.config.Config
import org.apache.pulsar.client.admin.{Namespaces, PulsarAdmin, PulsarAdminException, Topics}
import org.apache.pulsar.client.api._
import org.apache.pulsar.common.policies.data.RetentionPolicies

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class PulsarController(conf: Config) {
  private val pulsarAddress: String = conf.getString("raphtory.pulsar.broker.address")
  val pulsarAdminAddress: String    = conf.getString("raphtory.pulsar.admin.address")
  val retentionTimeout: Int         = conf.getString("raphtory.pulsar.retention.time").toInt
  val retentionSize: Int         = conf.getString("raphtory.pulsar.retention.size").toInt
  val prefixTopic: String        = "persistent://public/"

  private val client: PulsarClient  =
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
    println(s"Setting retention namespace for : ${namespace}, for time: ${retentionTime}, for size: ${retentionSize}")
    val policies = new RetentionPolicies(retentionTime, retentionSize)
    pulsarAdmin.namespaces.setRetention(namespace, policies)
  }

  def setRetentionTopic(topic: String, retentionTime: Int = retentionTimeout, retentionSize: Int = retentionSize
  ): Unit = {
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
                .maxNumMessages(10000)
                .timeout(1, TimeUnit.NANOSECONDS)
                .build()
      )
      .poolMessages(true)
      .messageListener(messageListener)
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
    client.newProducer(schema).topic(topic).blockIfQueueFull(true).create() //.enableBatching(true)

  def createTopic(topicName: String, deploymentID: String) : String = {
    val setRetention = createNamespace(deploymentID)
    if(setRetention) setRetentionNamespace(s"public/${deploymentID}")
    val topic = s"${prefixTopic}${deploymentID}/${topicName}"
    //pulsarAdmin.topics.createNonPartitionedTopic(topic)
    topic
  }

  def deleteTopic(topic: String) = {
    pulsarAdmin.topics().delete(topic)
  }

  def createNamespace(namespace: String) : Boolean = {
    var setRetention = true
    try pulsarAdmin.namespaces().createNamespace(s"public/${namespace}")
    catch {
      case error: PulsarAdminException =>
        println("Namespace already found")
        setRetention = false
    }
    setRetention
  }

  def setupComponent(deploymentID: String) : Unit = {
    val setRetention = createNamespace(deploymentID)
    if(setRetention) setRetentionNamespace(s"public/${deploymentID}")
  }

}
