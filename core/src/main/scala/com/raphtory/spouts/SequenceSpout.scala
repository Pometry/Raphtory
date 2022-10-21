package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance

/** Spout that takes an Iterable or Iterator and pushes the values into Raphtory
  */
class SequenceSpout[T](seq: IterableOnce[T]) extends Spout[T] {
  override def buildSpout(): SpoutInstance[T] = new SequenceSpoutInstance(seq)
}

object SequenceSpout {

  /** Create sequence spout from arguments
    *
    * @return `SequenceSpout` that returns the input arguments one-by-one
    */
  def apply[T](first: T, seq: T*): SequenceSpout[T] = new SequenceSpout[T](first +: seq)

  /** Create sequence spout from existing `Iterable` or `Iterator`
    * @return `SequenceSpout` wrapping the input
    */
  def apply[T](seq: IterableOnce[T]) = new SequenceSpout[T](seq)
}

class SequenceSpoutInstance[T](seqInput: IterableOnce[T]) extends SpoutInstance[T] {
  private val seqIterator                  = seqInput.iterator
  override def spoutReschedules(): Boolean = false

  override def hasNext: Boolean = seqIterator.hasNext

  override def next(): T = seqIterator.next()
}
