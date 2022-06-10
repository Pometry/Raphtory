package com.raphtory.api.output.sink

import com.raphtory.api.output.format.Format
import com.typesafe.config.Config

/** Base trait for sinks that use a generic [[com.raphtory.api.output.format.Format Format]] to write the data.
  *
  * This trait allows defining a [[Sink]] just by overriding the `buildConnector` method
  * @param format the [[com.raphtory.api.output.format.Format Format]]
  * @see [[com.raphtory.api.output.format.Format Format]]
  */
abstract class FormatAgnosticSink(format: Format) extends Sink {

  /** Builds a [[com.raphtory.api.output.sink.SinkConnector SinkConnector]] to be used by Raphtory for
    * writing a table using the provided [[com.raphtory.api.output.format.Format Format]].
    * @param jobID The ID of the job that generated the table
    * @param partitionID The ID of the partition of the table
    * @param config The configuration provided by the user
    * @param itemDelimiter The `String` to be used as delimiter between items when necessary
    * @return The [[com.raphtory.api.output.sink.SinkConnector SinkConnector]] implementing the execution of this `FormatAgnosticSink`
    *
    * @see [[com.raphtory.api.output.sink.SinkConnector SinkConnector]]
    */
  def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector

  final override def executor(
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor = {
    val connector =
      buildConnector(jobID, partitionID, config, format.defaultDelimiter)
    format.executor(connector, jobID, partitionID, config)
  }
}
