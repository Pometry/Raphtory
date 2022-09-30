package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.api.querytracker.QueryProgressTrackerBase
import com.raphtory.sinks.FileSink

trait TableBase {
  def filter(f: Row => Boolean): TableBase

  /** Explode table rows
    *
    * This creates a new table where each row in the old table
    * is mapped to multiple rows in the new table.
    *
    * @param f function that runs once for each row of the table and maps it to new rows
    */
  def explode(f: Row => IterableOnce[Row]): TableBase

  /** Write out data and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with custom job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    * @param jobName Name for job
    */
  def writeTo(sink: Sink, jobName: String): QueryProgressTrackerBase

  /** Write out data and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    */
  def writeTo(sink: Sink): QueryProgressTrackerBase

  /** Write out data to files and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param name folder path for writing results
    */
  def writeToFile(name: String): QueryProgressTrackerBase =
    writeTo(FileSink(name))

}
