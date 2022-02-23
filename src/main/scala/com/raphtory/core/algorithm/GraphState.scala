package com.raphtory.core.algorithm

import scala.collection.mutable
import scala.reflect.runtime.universe._

abstract class Accumulator[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) {
  var value: T = initialValue

  def +=(newValue: T): Unit
}

private class AccumulatorImplementation[T](
    initialValue: T,
    retainState: Boolean = false,
    op: (T, T) => T
) extends Accumulator[T](initialValue, retainState, op) {
  var currentValue: T = initialValue

  def +=(newValue: T): Unit =
    currentValue = op(currentValue, newValue)

  def reset(): Unit = {
    if (retainState)
      value = op(value, currentValue)
    else
      value = currentValue
    currentValue = initialValue
  }
}

private object AccumulatorImplementation {

  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new AccumulatorImplementation[T](initialValue, retainState, op)
}

class GraphState {
  private val state = mutable.Map.empty[String, AccumulatorImplementation[Any]]

  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState, op)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String)(implicit numeric: Numeric[T]): Unit =
    state(name) =
      AccumulatorImplementation[T](initialValue = numeric.zero, retainState = false, numeric.plus)
        .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, retainState: Boolean)(implicit numeric: Numeric[T]): Unit =
    state(name) =
      AccumulatorImplementation[T](initialValue = numeric.zero, retainState, numeric.plus)
        .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, initialValue: T)(implicit numeric: Numeric[T]): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, false, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def update(graphState: GraphState): Unit =
    graphState.state.foreach {
      case (name, value) =>
        state(name) += value.currentValue
    }

  def rotate(): Unit                                                  =
    state.foreach { case (name, accumulator) => accumulator.reset() }

  def apply[T](name: String)(implicit ev: TypeTag[T]): Accumulator[T] =
    state(name).asInstanceOf[Accumulator[T]]
}

object GraphState {
  def apply() = new GraphState
}
