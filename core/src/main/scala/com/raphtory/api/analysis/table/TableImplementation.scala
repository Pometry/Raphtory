package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.progresstracker.ProgressTracker
import com.raphtory.api.progresstracker.QueryProgressTracker
import com.raphtory.api.progresstracker.QueryProgressTrackerWithIterator
import com.raphtory.internals.components.output.TableOutputSink
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management._

import scala.concurrent.duration.Duration

private[api] class TableImplementation(val query: Query, private[raphtory] val querySender: QuerySender) extends Table {

  override def withDefaults(defaults: Map[String, Any]): Table =
    new TableImplementation(
            query.copy(defaults = defaults),
            querySender
    )

  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v: Row): Boolean = f(v)
    addFunction(TableFilter(closurefunc))
  }

  override def explode(f: Row => IterableOnce[Row]): Table = {
    def closurefunc(v: Row): IterableOnce[Row] = f(v)
    addFunction(Explode(closurefunc))
  }

  override def explode(column: String): Table =
    addFunction(ExplodeColumn(column))

  override def renameColumn(columns: (String, String)*): Table =
    addFunction(RenameColumn(columns))

  override def writeTo(sink: Sink, jobName: String): QueryProgressTracker =
    submitQueryWithSink(sink, jobName, jobID => querySender.createQueryProgressTracker(jobID))
      .asInstanceOf[QueryProgressTracker]

  override def writeTo(sink: Sink): QueryProgressTracker =
    writeTo(sink, "")

  override def get(jobName: String = "", timeout: Duration = Duration.Inf): Iterator[TableOutput] =
    submitQueryWithSink(
            TableOutputSink(querySender.graphID),
            jobName,
            jobID => querySender.createTableOutputTracker(jobID, timeout, query.header)
    )
      .asInstanceOf[QueryProgressTrackerWithIterator]
      .TableOutputIterator

  private def addFunction(function: TableFunction) =
    new TableImplementation(
            query.copy(operations = query.operations :+ function),
            querySender
    )

  private def submitQueryWithSink(
      sink: Sink,
      jobName: String,
      createProgressTracker: String => ProgressTracker
  ): ProgressTracker = {
    // val closedQuery     = addFunction(WriteToOutput).query -> Writing is not a operation in the list anymore
    val queryWithFormat = query.copy(sink = Some(sink))
    querySender.submit(queryWithFormat, jobName, createProgressTracker)
  }
}
