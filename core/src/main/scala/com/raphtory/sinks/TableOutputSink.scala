package com.raphtory.sinks

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.api.time.Interval
import com.raphtory.api.time.Perspective
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.ExclusiveTopic
import com.raphtory.internals.communication.TopicRepository
import com.typesafe.config.Config

sealed trait OutputMessages
case class RowOutput(timestamp: Long, window: Option[Interval], row: Row) extends OutputMessages
case object EndOutput                                                     extends OutputMessages

class TableOutputSinkExecutor(endPoint: EndPoint[OutputMessages]) extends SinkExecutor {
  private var timestamp: Long          = _
  private var window: Option[Interval] = _

  /** Sets up the perspective to be written out.
    * This method gets called every time a new graph perspective is going to be written out so this `SinkExecutor` can
    * handle it if needed.
    *
    * @param perspective the perspective to be written out
    */
  override def setupPerspective(perspective: Perspective): Unit = {
    timestamp = perspective.timestamp
    window = perspective.window
  }

  /** Writes out one row.
    * The implementation of this method doesn't need to be thread-safe as it is wrapped by `threadSafeWriteRow` to
    * handle synchronization.
    *
    * @param row the row of data to write out
    */
  override protected def writeRow(row: Row): Unit = endPoint.sendAsync(RowOutput(timestamp, window, row))

  /** Closes the writing of the current graph perspective.
    * This method gets called every time all the rows from one graph perspective have been successfully written out so
    * this `SinkExecutor` can handle it if needed.
    */
  override def closePerspective(): Unit = endPoint.flushAsync()

  /** Closes this `SinkExecutor` after writing the complete table.
    *
    * This method should free up all the resources in use.
    */
  override def close(): Unit = endPoint.closeWithMessage(EndOutput)
}

case object TableOutputSink extends Sink {

  /**
    * @param jobID       the ID of the job that generated the table
    * @param partitionID the ID of the partition of the table
    * @param config      the configuration provided by the user
    * @return the `SinkExecutor` to be used for writing out results
    */
  override def executor(jobID: String, partitionID: Int, config: Config, topics: TopicRepository): SinkExecutor =
    new TableOutputSinkExecutor(topics.output.endPoint)
}
