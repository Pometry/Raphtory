package com.raphtory.api

import com.raphtory.api.table.Row
import com.raphtory.time.Interval
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Interface for output formats.
  * Concrete implementations need to override the `write` method to output the data.
  *
  *  @see [[com.raphtory.output.FileOutputFormat]], [[com.raphtory.output.PulsarOutputFormat]],
  *  [[Row]]
  */
abstract class OutputFormat extends Serializable {

  /** Logger instance for writing debug messages */
  lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Write out tabular data
    *
    * @param timestamp timestamp for current graph perspective
    * @param window  window of current perspective (if set)
    * @param jobID ID of job that generated the data
    * @param row row of data to write out
    * @param partitionID ID of partition trying to write the data
    */
  def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit
}
