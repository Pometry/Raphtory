package com.raphtory.sinks

import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.MessageSinkConnector
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.formats.CsvFormat
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.management.client.GraphDeployment
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
  *  @see [[Sink]]
  *       [[GraphDeployment]]
  *       [[com.raphtory.deployment.Raphtory]]
  */
case class PulsarSink(topic: String, format: Format = CsvFormat())
        extends FormatAgnosticSink(format) {

  override protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector =
    new MessageSinkConnector {
      private val client                            = new PulsarConnector(config).accessClient
      private lazy val stringProducer               = client.newProducer(Schema.STRING).topic(topic).create()
      override def sendAsync(message: String): Unit = stringProducer.sendAsync(message)
      override def close(): Unit                    = client.close()
    }
}
