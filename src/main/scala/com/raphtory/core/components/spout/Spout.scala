package com.raphtory.core.components.spout

import com.raphtory.core.components.graphbuilder.GraphBuilder

import scala.reflect.runtime.universe._

trait Spout[T] extends Iterator[T] {
  def hasNextIterator(): Boolean    = hasNext
  def nextIterator(): Iterator[T]   = this
  var graphBuilder: GraphBuilder[T] = _

  def hasNext(): Boolean
  def next(): T

  private[core] def setBuilder(gb: GraphBuilder[T]) = graphBuilder = gb

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}
}
