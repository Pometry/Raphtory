package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.QuerySender
import com.raphtory.sinks.TableOutputSink

private[api] class TableImplementation(val query: Query, private val querySender: QuerySender) extends Table {

  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v: Row): Boolean = f(v)
    addFunction(TableFilter(closurefunc))
  }

  override def explode(f: Row => IterableOnce[Row]): Table = {
    def closurefunc(v: Row): IterableOnce[Row] = f(v)
    addFunction(Explode(closurefunc))
  }

  override def writeTo(sink: Sink, jobName: String): QueryProgressTracker = {
    val closedQuery     = addFunction(WriteToOutput).query
    val queryWithFormat = closedQuery.copy(sink = Some(sink))
    querySender.submit(queryWithFormat, jobName)
  }

  override def writeTo(sink: Sink): QueryProgressTracker =
    writeTo(sink, "")

  override def collect(jobName: String = ""): TableOutput =
    querySender.outputCollector(writeTo(TableOutputSink, jobName))

  private def addFunction(function: TableFunction) =
    new TableImplementation(
            query.copy(tableFunctions = query.tableFunctions.enqueue(function)),
            querySender
    )

}
