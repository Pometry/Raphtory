package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker.QueryProgressTracker
import com.raphtory.internals.components.querymanager.Operation

import scala.concurrent.duration.Duration

sealed private[raphtory] trait TableFunction                            extends Operation
final private[raphtory] case class TableFilter(f: (Row) => Boolean)     extends TableFunction
final private[raphtory] case class Explode(f: Row => IterableOnce[Row]) extends TableFunction

/**  Interface for table operations
  *
  * @see [[Row]], [[com.raphtory.api.output.sink.Sink Sink]], [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
  */
trait Table extends TableBase {

  //TODO
  def withDefaults(defaults: Map[String, Any]): Table

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
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with custom job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    * @param jobName Name for job
    */
  def writeTo(sink: Sink, jobName: String): QueryProgressTracker

  /** Write out data and
    * return [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
    * with default job name
    *
    * @param sink [[com.raphtory.api.output.sink.Sink Sink]] for writing results
    */
  def writeTo(sink: Sink): QueryProgressTracker

  def get(jobName: String = "", timeout: Duration = Duration.Inf): Iterator[TableOutput]
}
