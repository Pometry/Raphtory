package com.raphtory.core.components.spout

import com.raphtory.core.components.graphbuilder.GraphBuilder

import scala.reflect.runtime.universe._

trait Spout[T] {

  var graphBuilder: GraphBuilder[T] = _

  def hasNext(): Boolean
  def next(): T

  def hasNextIterator(): Boolean
  def nextIterator(): Iterator[T]
  def executeNextIterator(): Unit
  private[core] def setBuilder(gb: GraphBuilder[T]) = graphBuilder = gb

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}

}
