package com.raphtory.components.spout

import com.raphtory.Raphtory
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.api.input.GraphBuilder.assignID
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.serialisers.KryoSerialiser
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.scalatest.BeforeAndAfter
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

//Running this breaks all other tests, why?
@DoNotDiscover
class LOTRGraphBuilderTest extends AnyFunSuite with BeforeAndAfter {

  final val test_producer_topic: String      = "_test_lotr_graph_input_topic"
  final val test_graph_builder_topic: String = "_test_lotr_graph_object_topic"

  val config: Config = Raphtory.getDefaultConfig(
          Map[String, Any](
                  ("raphtory.spout.topic", test_producer_topic),
                  ("raphtory.deploy.id", test_graph_builder_topic),
                  ("raphtory.deploy.id", test_graph_builder_topic)
          ),
          distributed = false
  )

  val admin: PulsarAdmin                           =
    PulsarAdmin.builder.serviceHttpUrl(config.getString("raphtory.pulsar.admin.address")).build
  implicit private val schema: Schema[Array[Byte]] = Schema.BYTES
  val pulsarConnector                              = new PulsarConnector(config)
  val kryo                                         = new KryoSerialiser()

  def deleteTestTopic(): Unit = {
    val all_topics = admin.topics.getList("public/default")
    all_topics.forEach(topic_name =>
      try {
        if ((topic_name contains test_producer_topic) || (topic_name contains test_graph_builder_topic))
          admin.topics.unloadAsync(topic_name)
        admin.topics.delete(topic_name, true)
      }
      catch {
        case e: PulsarAdminException =>
      }
    )
  }

  before {
    deleteTestTopic()
  }

  after {
    deleteTestTopic()
  }

  try {
    // first create a local spout and ingest some local data
    deleteTestTopic()
    val client         = pulsarConnector.accessClient
    val producer_topic = test_producer_topic
    val producer       = client.newProducer(Schema.BYTES).topic(producer_topic).create()

    producer.sendAsync(kryo.serialise("Gandalf,Benjamin,400"))

    val srcID              = assignID("Gandalf")
    val tarID              = assignID("Benjamin")
    val messageAddExp      = VertexAdd(
            400,
            srcID,
            Properties(ImmutableProperty("name", "Gandalf")),
            Some(Type("Character"))
    )
    val messageAddExp2     = VertexAdd(
            400,
            tarID,
            Properties(ImmutableProperty("name", "Benjamin")),
            Some(Type("Character"))
    )
    val messageEdgeExp     =
      EdgeAdd(400, srcID, tarID, Properties(), Some(Type("Character Co-occurence")))
    val graph_object_topic = test_graph_builder_topic + "_0"
    Raphtory.createGraphBuilder(new LOTRGraphBuilder())

    // finally check that they created the objects

    val consumer       = client
      .newConsumer(Schema.BYTES)
      .subscriptionName("_test_lotr_graph_output_sub")
      .topic(graph_object_topic)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscribe()
    val messageAdd     = consumer.receive
    consumer.acknowledge(messageAdd)
    val messageAdd2    = consumer.receive
    consumer.acknowledge(messageAdd2)
    val messageAddEdge = consumer.receive
    consumer.acknowledge(messageAddEdge)

    test("LOTRGraphBuilder: e2e Graph builder Produces first vertex") {
      val msg = kryo.deserialise[GraphUpdate](messageAdd.getValue)
      assert(msg == messageAddExp, "Vertex Add Gandalf")
    }
    test("LOTRGraphBuilder: e2e Graph builder Produces second vertex") {
      val msg = kryo.deserialise[GraphUpdate](messageAdd2.getValue)
      assert(msg == messageAddExp2, "Vertex Add Benjamin")
    }
    test("LOTRGraphBuilder: e2e Graph builder Produces edge") {
      val msg = kryo.deserialise[GraphUpdate](messageAddEdge.getValue)
      assert(msg == messageEdgeExp, "Edge Add Gandalf -> Benjamin")
    }
    producer.close()
    consumer.close()
    client.close()
  }
  catch {
    case e: Exception =>
      e.printStackTrace()
      assert(false)
  }

}
