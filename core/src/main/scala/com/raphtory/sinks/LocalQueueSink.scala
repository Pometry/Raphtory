package com.raphtory.sinks

import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.formats.CsvFormat
import com.typesafe.config.Config

import java.util.concurrent.ArrayBlockingQueue

class LocalQueueSink(q: ArrayBlockingQueue[String], format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  /** Builds a [[com.raphtory.api.output.sink.SinkConnector SinkConnector]] to be used by Raphtory for
    * writing a table using the provided [[com.raphtory.api.output.format.Format Format]].
    *
    * @param jobID         The ID of the job that generated the table
    * @param partitionID   The ID of the partition of the table
    * @param config        The configuration provided by the user
    * @param itemDelimiter The `String` to be used as a delimiter between items when necessary
    * @return The [[com.raphtory.api.output.sink.SinkConnector SinkConnector]] implementing the execution of this `FormatAgnosticSink`
    * @see [[com.raphtory.api.output.sink.SinkConnector SinkConnector]]
    */
  override def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String,
      fileExtension: String
  ): SinkConnector =
    new SinkConnector {

      /** Appends a `value` to the current item
        *
        * @param value the value to append
        */
      override def write(value: String): Unit = q.offer(value)

      /** Completes the writing of the current item */
      override def closeItem(): Unit = {}

      /** Ensures that the output of this sink completed and frees up all the resources used by it. */
      override def close(): Unit = {}
    }
}
