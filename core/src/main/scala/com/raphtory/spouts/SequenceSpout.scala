package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance

/** Output input messages to Raphtory.
  * This spout is mainly useful for testing functionality
  */
class SequenceSpout[T](seq: IterableOnce[T]) extends Spout[T] {
  override def buildSpout(): SpoutInstance[T] = new SequenceSpoutInstance(seq)
}

object SequenceSpout {
  def apply[T](first: T, seq: T*): SequenceSpout[T] = new SequenceSpout[T](first +: seq)
  def apply[T](seq: IterableOnce[T])                = new SequenceSpout[T](seq)
}

class SequenceSpoutInstance[T](seqInput: IterableOnce[T]) extends SpoutInstance[T] {
  private val seqIterator                  = seqInput.iterator
  override def spoutReschedules(): Boolean = false

  override def hasNext: Boolean = seqIterator.hasNext

  override def next(): T = seqIterator.next()
}
