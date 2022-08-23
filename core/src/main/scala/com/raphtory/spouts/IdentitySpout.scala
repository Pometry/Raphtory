package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance

/**
  * A [[com.raphtory.api.input.Spout Spout]] that provides no data.
  *
  * This Spout is the default used for any Raphtory deployment. Is used if other services outside of Raphtory are communicating with the Graph Builder.
  *  For instance, another process may publish to the pulsar topic the GraphBuilders are listening to.
  *
  *  @see [[com.raphtory.api.input.Spout Spout]]
  *       [[com.raphtory.Raphtory Raphtory]]
  */
class IdentitySpout[T]() extends Spout[T] {
  override def buildSpout(): SpoutInstance[T] = new IdentitySpoutInstance()
}

class IdentitySpoutInstance[T]() extends SpoutInstance[T] {

  override def hasNext: Boolean = false

  override def next(): T = ???

  override def spoutReschedules(): Boolean = false

}
