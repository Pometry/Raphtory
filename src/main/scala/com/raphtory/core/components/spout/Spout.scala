package com.raphtory.core.components.spout

trait Spout[T] {

  def hasNext(): Boolean
  def next(): T

  def hasNextIterator(): Boolean
  def nextIterator(): Iterator[T]

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}

}
