package com.raphtory.spouts

import com.raphtory.api.input.Spout

class IdentitySpout[T]() extends Spout[T] {

  override def hasNext: Boolean = false

  override def next(): T = ???

  override def spoutReschedules(): Boolean = false

}
