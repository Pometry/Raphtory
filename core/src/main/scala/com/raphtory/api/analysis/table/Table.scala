package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querytracker.QueryProgressTracker
sealed trait TableFunction extends QueryManagement

final case class TableFilter(f: (Row) => Boolean)     extends TableFunction
final case class Explode(f: Row => IterableOnce[Row]) extends TableFunction
case object WriteToOutput                             extends TableFunction

/**  Interface for table operations
  *
  * @see [[Row]], [[Sink]], [[QueryProgressTracker]]
  */
abstract class Table {

  /** add a filter operation to table
    * @param f function that runs once for each row (only rows for which `f ` returns `true` are kept)
    */
  def filter(f: Row => Boolean): Table

  /** add an explode operation to table. This creates a new table where each row in the old table
    *      is mapped to multiple rows in the new table.
    * @param f function that runs once for each row and returns a list of new rows
    */
  def explode(f: Row => IterableOnce[Row]): Table

  /** write out data based on [[Sink]] and
    *    returns [[QueryProgressTracker]]
    */
  def writeTo(sink: Sink, jobName: String): QueryProgressTracker

  /** Blank write to allows usage from python api
    * @see [[Table.writeTo(outputFormat,jobName]]
    */
  def writeTo(sink: Sink): QueryProgressTracker
}
