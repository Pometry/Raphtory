package com.raphtory.sinks

import com.raphtory.api.analysis.table.Table
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.MessageSinkConnector
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.formats.CsvFormat
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.management.GraphDeployment
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

/** A `Sink` that sends a `Table` to the specified Pulsar `topic` using the given `format`.
  *
  * @param topic the name of the Pulsar topic to send the table to
  * @param format the format to be used by this sink (`CsvFormat` by default)
  *
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.PulsarSink
  * import com.raphtory.components.spout.instance.ResourceSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(ResourceSpout("resource"), graphBuilder)
  * val topic = "test"
  * val sink = PulsarSink(topic)
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[Sink]]
  *      [[Format]]
  *      [[CsvFormat]]
  *      [[Table]]
  *      [[com.raphtory.Raphtory]]
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
