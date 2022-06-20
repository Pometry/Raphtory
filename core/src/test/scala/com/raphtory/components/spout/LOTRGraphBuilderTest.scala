package com.raphtory.components.spout

import cats.effect.IO
import cats.effect.Resource
import cats.effect.SyncIO
import cats.implicits.toFoldableOps
import com.raphtory.Raphtory
import com.raphtory.RaphtoryService
import com.raphtory.api.input.GraphBuilder.assignID
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.typesafe.config.Config
import munit.CatsEffectSuite
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api._

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.CollectionHasAsScala

//Check if this test breaks stuff
class LOTRGraphBuilderTest extends CatsEffectSuite {

  private val _test_lotr_graph_object_topic = "_test_lotr_graph_object_topic"

  val pulsarWithBuilder: SyncIO[FunFixture[PulsarConnector]] =
    ResourceFixture(for {
      con <- PulsarConnector[IO](config)
      _   <- RaphtoryService.default(null, new LOTRGraphBuilder).builderDeploy(config, localDeployment = true)
      _   <- Resource.make(IO.pure(()))(_ => deleteTestTopic(con.adminClient))
    } yield con)

  def producerConsumerPair(
      client: PulsarClient,
      producerTopic: String,
      consumerTopic: String
  ): Resource[IO, (Producer[Array[Byte]], Consumer[Array[Byte]])] =
    for {
      p <- Resource.fromAutoCloseable(IO.delay(makeProducer(client, producerTopic)))
      c <- Resource.make(IO.delay(makeConsumer(client, consumerTopic)))(c =>
             IO.fromCompletableFuture(IO.delay(c.unsubscribeAsync())).void *> IO
               .fromCompletableFuture(IO.delay(c.closeAsync()))
               .void
           )
    } yield (p, c)

  pulsarWithBuilder.test("Receive messages from GraphBuilder 2 add VertexAdd and 1 EdgeAdd") { con =>
    val client = con.accessClient
    val kryo   = new KryoSerialiser()

    val producer_topic     = s"persistent://public/${_test_lotr_graph_object_topic}_0/${_test_lotr_graph_object_topic}_0"
    val graph_object_topic =
      s"persistent://public/${_test_lotr_graph_object_topic}_0/graph.updates-${_test_lotr_graph_object_topic}_0-0"

    producerConsumerPair(client, producer_topic, graph_object_topic).use {
      case (producer, consumer) =>
        IO.blocking {

          // first create a local spout and ingest some local data
          val doneSending = producer.sendAsync(kryo.serialise("Gandalf,Benjamin,400"))
          doneSending.get(60, TimeUnit.SECONDS)

          val srcID          = assignID("Gandalf")
          val tarID          = assignID("Benjamin")
          val messageAddExp  = VertexAdd(
                  400,
                  srcID,
                  Properties(ImmutableProperty("name", "Gandalf")),
                  Some(Type("Character"))
          )
          val messageAddExp2 = VertexAdd(
                  400,
                  tarID,
                  Properties(ImmutableProperty("name", "Benjamin")),
                  Some(Type("Character"))
          )
          val messageEdgeExp =
            EdgeAdd(400, srcID, tarID, Properties(), Some(Type("Character Co-occurence")))

          // finally check that they created the objects

          val msgs = (0 until 3).map { _ =>
            val msg = consumer.receive()
            consumer.acknowledge(msg)
            msg
          }

          val actualMsgs: Set[GraphUpdate] = msgs.toList
            .map(_.getValue)
            .map(msgBytes => kryo.deserialise[GraphUpdate](msgBytes))
            .toSet

          val expectedMsgs: Set[GraphUpdate] = Set(messageAddExp, messageAddExp2, messageEdgeExp)

          assertEquals(actualMsgs, expectedMsgs)

        }

    }

  }

  private def makeProducer(client: PulsarClient, producer_topic: String) =
    client.newProducer(Schema.BYTES).topic(producer_topic).create()

  private def makeConsumer(client: PulsarClient, graph_object_topic: String) = {
    val consumer: Consumer[Array[Byte]] = client
      .newConsumer(Schema.BYTES)
      .subscriptionName("_test_lotr_graph_output_sub")
      .topic(graph_object_topic)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscribe()

    consumer
  }

  final def test_producer_topic: String      = _test_lotr_graph_object_topic
  final def test_graph_builder_topic: String = _test_lotr_graph_object_topic

  def config: Config =
    Raphtory.getDefaultConfig(
            Map[String, Any](
                    ("raphtory.spout.topic", test_producer_topic),
                    ("raphtory.deploy.id", test_graph_builder_topic),
                    ("raphtory.pulsar.broker.ioThreads", 1),
                    ("raphtory.builders.countPerServer", 1),
                    ("raphtory.partitions.countPerServer", 1)
            ),
            salt = Some(0),
            distributed = false
    )

  def deleteTestTopic(admin: PulsarAdmin): IO[Unit] = {
    val subjectTopic = for {
      tenant <- admin.tenants().getTenants.asScala
      ns     <- admin.namespaces().getNamespaces(tenant).asScala
      topic  <- admin.topics().getList(ns).asScala
      _       = println(topic)
      if topic.endsWith(s"${_test_lotr_graph_object_topic}_0")
    } yield topic

    subjectTopic
      .map { topicName =>
        IO.println(s"Deleting $topicName") *>
          IO.fromCompletableFuture(IO.delay(admin.topics().terminateTopicAsync(topicName)))
//        *> IO.fromCompletableFuture(IO.delay(admin.topics().deleteAsync(topicName)))
      }
      .toList
      .sequence_
  }

}
