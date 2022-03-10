package com.raphtory.core.components.spout

import scala.reflect.runtime.universe._

trait Spout[T] {

  def hasNext(): Boolean
  def next(): T

  def hasNextIterator(): Boolean
  def nextIterator(): Iterator[T]

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}

}
