package com.raphtory.core.algorithm

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

abstract class OutputFormat extends Serializable {
  lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def write(timestamp: Long, window: Option[Long], jobID: String, row: Row, partitionID: Int): Unit
}
