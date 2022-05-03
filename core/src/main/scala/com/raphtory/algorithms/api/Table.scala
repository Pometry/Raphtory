package com.raphtory.algorithms.api

import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querytracker.QueryProgressTracker

sealed trait TableFunction extends QueryManagement

final case class TableFilter(f: (Row) => Boolean)    extends TableFunction
final case class Explode(f: Row => List[Row])        extends TableFunction
final case class WriteTo(outputFormat: OutputFormat) extends TableFunction

/**
  * `Table`
  *  : Interface for table operations
  *
  * ## Methods
  *
  *  `filter(f: Row => Boolean): Table`
  *    : add a filter operation to table
  *
  *      `f: Row => Boolean`
  *        : function that runs once for each row (only rows for which `f ` returns `true` are kept)
  *
  *  `explode(f: Row => List[Row]): Table`
  *    : add an explode operation to table. This creates a new table where each row in the old table
  *      is mapped to multiple rows in the new table.
  *
  *      `f: Row => List[Row]`
  *        : function that runs once for each row and returns a list of new rows
  *
  *  `writeTo(outputFormat: OutputFormat): QueryProgressTracker`
  *    : write out data based on [`outputFormat`](com.raphtory.algorithms.api.OutputFormat) and
  *    returns [`QueryProgressTracker`](com.raphtory.components.querytracker.QueryProgressTracker)
  *
  *  ```{seealso}
  *  [](com.raphtory.algorithms.api.Row), [](com.raphtory.algorithms.api.OutputFormat), [](com.raphtory.components.querytracker.QueryProgressTracker)
  *  ```
  */
abstract class Table {
  def filter(f: Row => Boolean): Table
  def explode(f: Row => List[Row]): Table
  def writeTo(outputFormat: OutputFormat, jobName: String): QueryProgressTracker
  def writeTo(outputFormat: OutputFormat): QueryProgressTracker // blank write to allows usage from python api
}
