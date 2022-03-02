package com.raphtory.core.algorithm

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querytracker.QueryProgressTracker

sealed trait TableFunction extends QueryManagement

final case class TableFilter(f: (Row) => Boolean)    extends TableFunction
final case class Explode(f: Row => List[Row])        extends TableFunction
final case class WriteTo(outputFormat: OutputFormat) extends TableFunction

abstract class Table {
  def filter(f: Row => Boolean): Table
  def explode(f: Row => List[Row]): Table
  def writeTo(outputFormat: OutputFormat): QueryProgressTracker
}
