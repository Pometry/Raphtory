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

private object Accumulator {
  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new Accumulator[T](initialValue, retainState, op)
}


class GraphState {
  private val state = mutable.Map.empty[String, Accumulator[_]]

  def newState(name: String, initialValue: Int = 0, retainState: Boolean = false, op: (Int, Int) => Int = _ + _): Unit = {
    state(name) = Accumulator(initialValue, retainState, op)
  }

  def newState(name: String, initialValue: Long = 0, retainState: Boolean = false, op: (Long, Long) => Long = _ + _): Unit = {
    state(name) = Accumulator(initialValue, retainState, op)
  }

  def newState(name: String, initialValue: Double = 0, retainState: Boolean = false, op: (Double, Double) => Double = _ + _): Unit = {
    state(name) = Accumulator(initialValue, retainState, op)
  }

  def newState[T](name: String, initialValue: T, retainState: Boolean = false, op: (T, T) => T): Unit = {
    state(name) = Accumulator(initialValue, retainState, op)
  }

  def apply[T](name: String): Accumulator[T] = {
    state(name).asInstanceOf[Accumulator[T]]
  }
}

object GraphState {
  def apply() = new GraphState
}
