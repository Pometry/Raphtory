package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance

/** Output input messages to Raphtory.
  * This spout is mainly useful for testing functionality
  */
class SequenceSpout[T](seq: Seq[T]) extends Spout[T] {
  override def buildSpout(): SpoutInstance[T] = new SequenceSpoutInstance(seq)
}

object SequenceSpout {
  def apply[T](seq: T*): SequenceSpout[T] = new SequenceSpout[T](seq)
}

class SequenceSpoutInstance[T](seq: Seq[T]) extends SpoutInstance[T] {
  private val seqIterator                  = seq.iterator
  override def spoutReschedules(): Boolean = false

  override def hasNext: Boolean = seqIterator.hasNext

  override def next(): T = seqIterator.next()
}
