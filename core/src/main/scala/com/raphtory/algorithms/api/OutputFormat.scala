package com.raphtory.algorithms.api

import com.raphtory.graph.Perspective
import com.raphtory.output.Sink
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait OutputFormat {
  def outputWriter(jobId: String, partitionId: Int, config: Config): OutputWriter
}

/** Interface for output formats.
  * Concrete implementations need to override the `write` method to output the data.
  *
  *  @see [[com.raphtory.output.FileOutputFormat]], [[com.raphtory.output.PulsarOutputFormat]],
  *  [[com.raphtory.algorithms.api.Row]]
  */
trait OutputWriter {
  type OutputType
  protected val sink: Sink[OutputType] = createSink()

  /** Logger instance for writing debug messages */
  protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  final def writeRow(row: Row): Unit = synchronized(threadSafeWriteRow(row))

  def setupPerspective(perspective: Perspective): Unit
  def closePerspective(): Unit
  def close(): Unit
  protected def createSink(): Sink[OutputType]
  protected def threadSafeWriteRow(row: Row): Unit
}
