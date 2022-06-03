package com.raphtory.api.table

import com.raphtory.api.OutputFormat
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querytracker.QueryProgressTracker

sealed trait TableFunction extends QueryManagement

final case class TableFilter(f: (Row) => Boolean)    extends TableFunction
final case class Explode(f: Row => List[Row])        extends TableFunction
final case class WriteTo(outputFormat: OutputFormat) extends TableFunction

/**  Interface for table operations
  *
  * @see [[Row]], [[OutputFormat]], [[com.raphtory.components.querytracker.QueryProgressTracker]]
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
  def explode(f: Row => List[Row]): Table

  /** write out data based on [[OutputFormat]] and
    *    returns [[com.raphtory.components.querytracker.QueryProgressTracker]]
    */
  def writeTo(outputFormat: OutputFormat, jobName: String): QueryProgressTracker

  /** Blank write to allows usage from python api
    * @see [[Table.writeTo(outputFormat,jobName]]
    */
  def writeTo(outputFormat: OutputFormat): QueryProgressTracker
}
