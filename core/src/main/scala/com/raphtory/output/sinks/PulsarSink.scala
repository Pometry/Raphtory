package com.raphtory.output.sinks

import com.raphtory.communication.connectors.PulsarConnector
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

class PulsarSink(pulsarTopic: String, config: Config) extends AbstractMessageSink {

  private val producer = new PulsarConnector(config).accessClient
    .newProducer(Schema.STRING)
    .topic(pulsarTopic) // TODO change here : Topic name with deployment
    .create()

  override def sendAsync(message: String): Unit = producer.sendAsync(message)

  override def close(): Unit = producer.close()
}
