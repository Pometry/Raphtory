package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker.QueryProgressTracker
import com.raphtory.internals.components.querymanager.Operation

import scala.concurrent.duration.Duration

sealed private[raphtory] trait TableFunction                           extends Operation
final private[raphtory] case class TableFilter(f: (Row) => Boolean)    extends TableFunction
final private[raphtory] case class ExplodeColumns(column: Seq[String]) extends TableFunction

final private[raphtory] case class RenameColumn(columns: Seq[(String, String)]) extends TableFunction

/**  Interface for table operations
  *
  * @see [[Row]], [[com.raphtory.api.output.sink.Sink Sink]], [[com.raphtory.api.progresstracker.QueryProgressTracker QueryProgressTracker]]
  */
trait Table extends TableBase {
  override def filter(f: Row => Boolean): Table
  override def explode(columns: String*): Table
  override def renameColumns(columns: (String, String)*): Table
  override def writeTo(sink: Sink, jobName: String): QueryProgressTracker
  override def writeTo(sink: Sink): QueryProgressTracker
  def get(jobName: String = "", timeout: Duration = Duration.Inf): Iterator[TableOutput]
}
