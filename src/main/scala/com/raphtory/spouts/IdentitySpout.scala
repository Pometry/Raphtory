package com.raphtory.spouts

import com.raphtory.core.components.spout.Spout
import scala.reflect.runtime.universe.TypeTag

class IdentitySpout[T]() extends Spout[T] {
  override def hasNext(): Boolean         = false
  override def hasNextIterator(): Boolean = false

  override def next(): T                   = ???
  override def nextIterator(): Iterator[T] = ???

  override def spoutReschedules(): Boolean = false

  override def executeNextIterator(): Unit = ???
}
