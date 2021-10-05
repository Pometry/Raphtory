package com.raphtory.core.implementations.objectgraph.algorithm

import com.raphtory.core.model.algorithm.{GraphFunction, Row, Table, TableFilter, TableFunction, WriteTo}

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

  def getNextOperation():Option[TableFunction] = if (tableOpps.nonEmpty) Some(tableOpps.dequeue()) else None
}
