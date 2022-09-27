package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.components.output.TableOutputSink
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.QuerySender

import scala.concurrent.duration.Duration

private[api] class TableImplementation(val query: Query, private[raphtory] val querySender: QuerySender) extends Table {

  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v: Row): Boolean = f(v)
    addFunction(TableFilter(closurefunc))
  }

  override def explode(f: Row => IterableOnce[Row]): Table = {
    def closurefunc(v: Row): IterableOnce[Row] = f(v)
    addFunction(Explode(closurefunc))
  }

  override def writeTo(sink: Sink, jobName: String): QueryProgressTracker = {
    val jobID = submitQueryWithSink(sink, jobName)
    querySender.createTracker(jobID)
  }

  override def writeTo(sink: Sink): QueryProgressTracker =
    writeTo(sink, "")

  override def get(jobName: String = "", timeout: Duration = Duration.Inf): TableOutputTracker =
    querySender.outputCollector(submitQueryWithSink(TableOutputSink, jobName), timeout)

  private def addFunction(function: TableFunction) =
    new TableImplementation(
            query.copy(tableFunctions = query.tableFunctions.enqueue(function)),
            querySender
    )

  private def submitQueryWithSink(sink: Sink, jobName: String): String = {
    val closedQuery     = addFunction(WriteToOutput).query
    val queryWithFormat = closedQuery.copy(sink = Some(sink))
    querySender.submit(queryWithFormat, jobName)
  }
}
