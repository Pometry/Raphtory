package com.raphtory.core.implementations.objectgraph.algorithm

import com.raphtory.core.model.algorithm.{Explode, GraphFunction, Row, Table, TableFilter, TableFunction, WriteTo}

import scala.collection.mutable

class ObjectTable extends Table{
  val tableOpps = mutable.Queue[TableFunction]()

  override def filter(f: Row => Boolean): Table = {
    tableOpps.enqueue(TableFilter(f))
    this
  }
  override def writeTo(address: String): Unit = {
    tableOpps.enqueue(WriteTo(address))
  }

  override def explode(f: Row => List[Row]): Table = {
    tableOpps.enqueue(Explode(f))
    this
  }

  def getNextOperation():Option[TableFunction] = if (tableOpps.nonEmpty) Some(tableOpps.dequeue()) else None
}
