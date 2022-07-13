package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.QuerySender

private[api] class TableImplementation[T](val query: Query, private val querySender: QuerySender) extends Table[T] {

  override def filter(f: T => Boolean): Table[T] =
    addFunction(TableFilter(f.asInstanceOf[Any => Boolean]))

  override def explode[Q](f: T => IterableOnce[Q]): Table[Q] =
    addFunction(Explode(f.asInstanceOf[Any => IterableOnce[Any]]))

  override def writeTo(sink: Sink, jobName: String): QueryProgressTracker = {
    val closedQuery     = addFunction(WriteToOutput).query
    val queryWithFormat = closedQuery.copy(sink = Some(sink))
    querySender.submit(queryWithFormat, jobName)
  }

  override def writeTo(sink: Sink): QueryProgressTracker =
    writeTo(sink, "")

  private def addFunction[Q](function: TableFunction) =
    new TableImplementation[Q](
            query.copy(tableFunctions = query.tableFunctions.enqueue(function)),
            querySender
    )
}
