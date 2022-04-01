package com.raphtory.components.spout

trait BatchableSpout[T] extends Spout[T] {
  def hasNextIterator(): Boolean
  def nextIterator(): Iterator[T]
}
