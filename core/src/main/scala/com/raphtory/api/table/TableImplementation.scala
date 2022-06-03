package com.raphtory.api.table

import com.raphtory.api.OutputFormat
import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.components.querytracker.QueryProgressTracker

/** @note DoNotDocument */
private[api] class TableImplementation(val query: Query, private val querySender: QuerySender)
        extends Table {

  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v: Row): Boolean = f(v)
    addFunction(TableFilter(closurefunc))
  }

  override def explode(f: Row => List[Row]): Table = {
    def closurefunc(v: Row): List[Row] = f(v)
    addFunction(Explode(closurefunc))
  }

  override def writeTo(outputFormat: OutputFormat, jobName: String): QueryProgressTracker = {
    val query = addFunction(WriteTo(outputFormat)).query
    querySender.submit(query, jobName)
  }

  override def writeTo(outputFormat: OutputFormat): QueryProgressTracker =
    writeTo(outputFormat, "")

  private def addFunction(function: TableFunction) =
    new TableImplementation(
            query.copy(tableFunctions = query.tableFunctions.enqueue(function)),
            querySender
    )
}
