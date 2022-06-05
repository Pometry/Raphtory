package com.raphtory.api.output.sink

import com.raphtory.api.analysis.table.Row
import com.raphtory.internal.graph.Perspective
import com.raphtory.sinks.FileSink
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Interface for output formats
  * Concrete implementations need to override the `outputWriter` method to create their own `OutputWriter`.
  *
  * @see [[FileSink]], [[com.raphtory.sinks.PulsarSink]],
  *      [[com.raphtory.api.table]]
  */
trait Sink {

  /**
    * @param jobId ID of the job that generated the data
    * @param partitionID ID of partition trying to write the data
    * @param config
    * @return `OutputWriter` to be used for writing out results
    */
  def executor(jobId: String, partitionID: Int, config: Config): SinkExecutor
}

/** Interface for output writers.
  * Concrete implementations need to override the `writeRow`, `setupPerspective`, `closePerspective`, and `close`
  * methods.
  *
  *  @see [[Row]]
  */
trait SinkExecutor {

  /** Logger instance for writing debug messages */
  protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Setup the perspective to be write out.
    * This method gets called every time a new graph perspective is going to be write out so this `OutputWriter` can
    * handle it if needed.
    * @param perspective perspective to be write out
    */
  def setupPerspective(perspective: Perspective): Unit

  /** Write out one row.
    * The implementation of this method doesn't need to be thread-safe as it is wrapped by `threadSafeWriteRow` to
    * handle synchronization.
    * @param row row of data to write out
    */
  protected def writeRow(row: Row): Unit

  /** Close the writing of the current graph perspective
    * This method gets called every time all the rows from one graph perspective have been successfully written out so
    * this `OutputWriter` can handle it if needed.
    */
  def closePerspective(): Unit

  /** Close this `OutputWriter` after writing the complete table
    */
  def close(): Unit

  /** Thread safe version of `writeRow` used internally by Raphtory.
    * @param row row of data to write out
    */
  final def threadSafeWriteRow(row: Row): Unit = synchronized(writeRow(row))
}
