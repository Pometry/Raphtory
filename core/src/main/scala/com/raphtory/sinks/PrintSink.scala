package com.raphtory.sinks

import com.raphtory.api.analysis.table.Table
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.StreamSinkConnector
import com.raphtory.formats.CsvFormat
import com.typesafe.config.Config

/** A `Sink` that prints a `Table` to the standard output.
  *
  * @param format the format to be used by this sink (`CsvFormat` by default)
  *
  * @note This sink is only intended for testing and should be used for deployments with only one partition.
  *
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.PrintSink
  * import com.raphtory.components.spout.instance.ResourceSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(ResourceSpout("resource"), graphBuilder)
  * val sink = PrintSink()
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[Sink]]
  *      [[Format]]
  *      [[CsvFormat]]
  *      [[Table]]
  *      [[com.raphtory.Raphtory]]
  */
case class PrintSink(format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  override protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector =
    new StreamSinkConnector(itemDelimiter) {
      override def output(value: String): Unit = System.out.print(value)
      override def close(): Unit               = System.out.println()
    }
}
