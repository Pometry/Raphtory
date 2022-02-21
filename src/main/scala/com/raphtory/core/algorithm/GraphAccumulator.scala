package com.raphtory.core.algorithm
import scala.collection.mutable
import scala.reflect.runtime.universe._


private class Accumulator[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) {
  var value: T = initialValue
  var lastValue: T = initialValue

  def += (newValue: T): Unit = {
    value = op(value, newValue)
  }

  def reset(): Unit = {
    lastValue = value
    if (! retainState) {
      value = initialValue
    }
  }
}

object Accumulator {
  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new Accumulator[T](initialValue, retainState, op)
}


class GraphAccumulator {
  private val state = mutable.Map.empty[String, Accumulator[_]]

  def newAccumulator[Int](name: String, initialValue: Int = 0, retainState: Boolean = false, op: (Int, Int) => Int = _ + _): Unit = {
    state(name) = Accumulator(initialValue, retainState, op)
  }

  def accumulate[T](name: String, value: T): Unit = 

}

object GraphAccumulator {
  def apply() = new GraphAccumulator
}
