package com.raphtory.pulsar.sink

import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.MessageSinkConnector
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.formats.CsvFormat
import com.raphtory.pulsar.connector.PulsarConnector
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

/** A [[com.raphtory.api.output.sink.Sink Sink]] that sends a `Table` to the specified Pulsar `topic` using the given `format`.
  *
  * @param topic the name of the Pulsar topic to send the table to
  * @param format the format to be used by this sink (`CsvFormat` by default)
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.PulsarSink
  * import com.raphtory.spouts.FileSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(FileSpout("/path/to/your/file"), graphBuilder)
  * val topic = "test"
  * val sink = PulsarSink(topic)
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[com.raphtory.api.output.sink.Sink Sink]]
  *      [[com.raphtory.api.output.format.Format Format]]
  *      [[com.raphtory.formats.CsvFormat CsvFormat]]
  *      [[com.raphtory.api.analysis.table.Table Table]]
  */
case class PulsarSink(topic: String, format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  override def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String,
      fileExtension: String
  ): SinkConnector =
    new MessageSinkConnector {
      private val conn                = PulsarConnector.unsafeApply(config)
      private val client              = conn.accessClient
      private val adminClient         = conn.adminClient
      private lazy val stringProducer = client.newProducer(Schema.STRING).topic(topic).create()

      override def allowsHeader: Boolean            = false
      override def sendAsync(message: String): Unit = stringProducer.sendAsync(message)

      override def close(): Unit = {
        client.close()
        adminClient.close()
      }
    }
}
