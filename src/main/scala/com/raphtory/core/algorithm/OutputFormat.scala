package com.raphtory.core.algorithm

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
  * # OutputFormat
  * The {s}`OutputFormat` writes Raphtory output to different streams eg. Pulsar or File
  *
  *  ## Methods
  *    {s}`write[T](timestamp: Long, window: Option[Long], jobID: String, row: Row, partitionID: Int): Unit`
  *      : Writes computed row for a partition for a specific window frame
  *
  *        {s}`timestamp: Long`
  *          : Timestamp for the write operation.
  *
  *        {s}`window: Option[Long]`
  *          : Window of start and end timestamps for which this row is computed.
  *
  *        {s}`jobID: String`
  *          : Job identifier for Raphtory job.
  *
  *        {s}`row: Row`
  *          : Row for computation.
  *
  *        {s}`partitionID: Int``
  *          : Paritition identifier.
  */
abstract class OutputFormat extends Serializable {
  lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def write(timestamp: Long, window: Option[Long], jobID: String, row: Row, partitionID: Int): Unit
}
