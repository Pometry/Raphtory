package com.raphtory.core.algorithm

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querytracker.QueryProgressTracker

sealed trait TableFunction extends QueryManagement

final case class TableFilter(f: (Row) => Boolean)    extends TableFunction
final case class Explode(f: Row => List[Row])        extends TableFunction
final case class WriteTo(outputFormat: OutputFormat) extends TableFunction

/**
  * {s}`Table`
  *  : Interface for table operations
  *
  * ## Methods
  *
  *  {s}`filter(f: Row => Boolean): Table`
  *    : add a filter operation to table
  *
  *      {s}`f: Row => Boolean`
  *        : function that runs once for each row (only rows for which {s}`f ` returns {s}`true` are kept)
  *
  *  {s}`explode(f: Row => List[Row]): Table`
  *    : add an explode operation to table. This creates a new table where each row in the old table
  *      is mapped to multiple rows in the new table.
  *
  *      {s}`f: Row => List[Row]`
  *        : function that runs once for each row and returns a list of new rows
  *
  *  {s}`writeTo(outputFormat: OutputFormat)`
  *    : write out data based on [{s}`outputFormat`](com.raphtory.core.algorithm.OutputFormat)
  *
  *  ```{seealso}
  *  [](com.raphtory.core.algorithm.Row), [](com.raphtory.core.algorithm.OutputFormat)
  *  ```
  */
abstract class Table {
  def filter(f: Row => Boolean): Table
  def explode(f: Row => List[Row]): Table
  def writeTo(outputFormat: OutputFormat): QueryProgressTracker
}
