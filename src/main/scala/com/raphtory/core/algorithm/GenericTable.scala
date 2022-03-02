package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.components.querytracker.QueryProgressTracker

/** @DoNotDocument */
class GenericTable(private val queryBuilder: QueryBuilder) extends Table {

  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v: Row): Boolean = f(v)
    addFunction(TableFilter(closurefunc))
  }

  override def explode(f: Row => List[Row]): Table = {
    def closurefunc(v: Row): List[Row] = f(v)
    addFunction(Explode(closurefunc))
  }

  override def writeTo(outputFormat: OutputFormat): QueryProgressTracker =
    queryBuilder.addTableFunction(WriteTo(outputFormat)).submit()

  private def addFunction(function: TableFunction) =
    new GenericTable(queryBuilder.addTableFunction(function))
}
