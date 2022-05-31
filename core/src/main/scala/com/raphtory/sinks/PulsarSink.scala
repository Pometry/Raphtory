package com.raphtory.sinks

import com.raphtory.communication.connectors.PulsarConnector
import com.raphtory.formats.CsvFormat
import com.raphtory.formats.Format
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

/** Writes the rows of a `Table` to the Pulsar topic specified by `pulsarTopic` in CSV format.
  *
  * @param pulsarTopic name of the pulsar topic
  *
  * Usage:
  * (while querying or running algorithmic tests)
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.algorithms.api.OutputFormat
  * import com.raphtory.components.spout.instance.ResourceSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(ResourceSpout("resource"), graphBuilder)
  * val testDir = "/tmp/raphtoryTest"
  * val outputFormat: OutputFormat = PulsarOutputFormat("edge-list-topic")
  *
  * graph.execute(EdgeList()).writeTo(outputFormat)
  * }}}
  *  @see [[com.raphtory.algorithms.api.Sink]]
  *       [[com.raphtory.client.GraphDeployment]]
  *       [[com.raphtory.deployment.Raphtory]]
  */
case class PulsarSink(pulsarTopic: String, format: Format[String] = CsvFormat())
        extends FormatAgnosticSink(format) {

  class PulsarSinkConnector(pulsarTopic: String, config: Config) extends MessageSinkConnector {

    private val producer = new PulsarConnector(config).accessClient
      .newProducer(Schema.STRING)
      .topic(pulsarTopic) // TODO change here : Topic name with deployment
      .create()

    override def sendAsync(message: String): Unit = producer.sendAsync(message)

    override def close(): Unit = producer.close()
  }

  override protected def createConnector(
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkConnector[String] =
    new PulsarSinkConnector(pulsarTopic, config)
}
