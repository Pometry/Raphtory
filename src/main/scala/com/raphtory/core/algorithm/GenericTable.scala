package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.components.querytracker.QueryProgressTracker

/** @DoNotDocument */
class GenericTable(val queryBuilder: QueryBuilder) extends Table {

  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v: Row): Boolean = f(v)
    addFunction(TableFilter(closurefunc))
  }

  override def explode(f: Row => List[Row]): Table = {
    def closurefunc(v: Row): List[Row] = f(v)
    addFunction(Explode(closurefunc))
  }

  override def writeTo(outputFormat: OutputFormat, jobName: String = ""): QueryProgressTracker =
    queryBuilder.addTableFunction(WriteTo(outputFormat)).submit(jobName)

  private def addFunction(function: TableFunction) =
    new GenericTable(queryBuilder.addTableFunction(function))
}
