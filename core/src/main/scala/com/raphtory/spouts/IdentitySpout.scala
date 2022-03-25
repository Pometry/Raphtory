package com.raphtory.spouts

import com.raphtory.components.spout.Spout

class IdentitySpout[T]() extends Spout[T] {

  override def hasNext: Boolean = false

  override def next(): T = ???

  override def spoutReschedules(): Boolean = false

}
