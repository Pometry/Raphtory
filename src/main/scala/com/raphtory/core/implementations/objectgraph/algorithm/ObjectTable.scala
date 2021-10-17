package com.raphtory.core.implementations.objectgraph.algorithm

import com.raphtory.core.model.algorithm.{Explode, GraphFunction, Row, Table, TableFilter, TableFunction, WriteTo}

import scala.collection.mutable

class ObjectTable extends Table{
  val tableOpps = mutable.Queue[TableFunction]()

  def bulkAdd(tableFuncs:List[TableFunction]) = tableFuncs.foreach(f=> tableOpps.enqueue(f))


  override def filter(f: Row => Boolean): Table = {
    def closurefunc(v:Row):Boolean = f(v)
    tableOpps.enqueue(TableFilter(closurefunc))
    this
  }
  override def writeTo(address: String): Unit = {
    tableOpps.enqueue(WriteTo(address))
  }

  override def explode(f: Row => List[Row]): Table = {
    def closurefunc(v:Row):List[Row] = f(v)
    tableOpps.enqueue(Explode(closurefunc))
    this
  }

  def getNextOperation():Option[TableFunction] = if (tableOpps.nonEmpty) Some(tableOpps.dequeue()) else None
}
