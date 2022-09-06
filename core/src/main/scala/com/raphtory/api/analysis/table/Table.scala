package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.sinks.FileSink
sealed private[raphtory] trait TableFunction extends QueryManagement

final private[raphtory] case class TableFilter(f: (Row) => Boolean)     extends TableFunction
final private[raphtory] case class Explode(f: Row => IterableOnce[Row]) extends TableFunction
private[raphtory] case object WriteToOutput                             extends TableFunction

/**  Interface for table operations
  *
  * @see [[Row]], [[com.raphtory.api.output.sink.Sink Sink]], [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
  */
trait Table {

  /** Add a filter operation to table
    * @param f function that runs once for each row (only rows for which `f ` returns `true` are kept)
    */
  def filter(f: Row => Boolean): Table

  /** Explode table rows
    *
    * This creates a new table where each row in the old table
    * is mapped to multiple rows in the new table.
    *
    * @param f function that runs once for each row of the table and maps it to new rows
    */
  def explode(f: Row => IterableOnce[Row]): Table

  /** Write out data and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with custom job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    * @param jobName Name for job
    */
  def writeTo(sink: Sink, jobName: String): QueryProgressTracker

  /** Write out data and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    */
  def writeTo(sink: Sink): QueryProgressTracker

  /** Write out data to files and
    * return [[com.raphtory.api.querytracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param name folder path for writing results
    */
  def writeToFile(name: String): QueryProgressTracker =
    writeTo(FileSink(name))

  def collect(jobName: String = ""): TableOutput
}
