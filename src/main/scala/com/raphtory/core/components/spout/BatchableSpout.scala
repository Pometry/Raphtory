package com.raphtory.core.components.spout

trait BatchableSpout[T] extends Spout[T] {
  def hasNextIterator(): Boolean
  def nextIterator(): Iterator[T]
  def executeNextIterator(): Unit
}
