package com.raphtory.api.table

import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querytracker.QueryProgressTracker
import com.raphtory.algorithms.api.Sink
sealed trait TableFunction extends QueryManagement

final case class TableFilter(f: (Row) => Boolean)     extends TableFunction
final case class Explode(f: Row => IterableOnce[Row]) extends TableFunction
case object WriteToOutput                             extends TableFunction

/**  Interface for table operations
  *
  * @see [[com.raphtory.api.table.Row]], [[com.raphtory.algorithms.api.Sink]], [[com.raphtory.components.querytracker.QueryProgressTracker]]
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

  /** write out data based on [[com.raphtory.algorithms.api.Sink]] and
    *    returns [[com.raphtory.components.querytracker.QueryProgressTracker]]
    */
  def writeTo(sink: Sink, jobName: String): QueryProgressTracker

  /** Blank write to allows usage from python api
    * @see [[Table.writeTo(outputFormat,jobName]]
    */
  def writeTo(sink: Sink): QueryProgressTracker
}
