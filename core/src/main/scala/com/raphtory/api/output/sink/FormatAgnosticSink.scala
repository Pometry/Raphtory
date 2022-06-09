package com.raphtory.api.output.sink

import com.raphtory.api.output.format.Format
import com.typesafe.config.Config

/** Base trait for sinks that use a generic `Format` to write the data.
  *
  * This trait allows defining a `Sink` just by overriding the `buildConnector` method
  * @param format the `Format`
  * @see [[Format]]
  */
abstract class FormatAgnosticSink(format: Format) extends Sink {

  /** Builds a `SinkConnector` to be used by Raphtory for writing a table using the provided `Format`.
    * @param jobID the ID of the job that generated the table
    * @param partitionID the ID of the partition of the table
    * @param config the configuration provided by the user
    * @param itemDelimiter the `String` to be used as delimiter between items when necessary
    * @return the `SinkConnector` implementing the execution of this `FormatAgnosticSink`
    *
    * @see [[SinkConnector]]
    */
  protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector

  final override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor = {
    val connector =
      buildConnector(jobID, partitionID, config, format.defaultDelimiter)
    format.executor(connector, jobID, partitionID, config)
  }
}
