package com.raphtory.core.algorithm
import scala.collection.mutable
import scala.reflect.runtime.universe._

class Accumulator[T](initialValue: T,
                     retainState: Boolean = false,
                     op: (T, T) => T) {
  var value: T = initialValue
  var lastValue: T = initialValue

  def +=(newValue: T): Unit = {
    value = op(value, newValue)
  }

  def reset(): Unit = {
    lastValue = value
    if (!retainState) {
      value = initialValue
    }
  }
}

private object Accumulator {
  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new Accumulator[T](initialValue, retainState, op)
}

class GraphState {
  private val state = mutable.Map.empty[String, Accumulator[Any]]

  def newAccumulator[T](name: String,
                        initialValue: T,
                        retainState: Boolean = false,
                        op: (T, T) => T): Unit = {
    state(name) = Accumulator[T](initialValue, retainState, op)
      .asInstanceOf[Accumulator[Any]]
  }

  def newAdder[T](name: String)(implicit numeric: Numeric[T]): Unit = {
    state(name) =
      Accumulator[T](initialValue = numeric.zero, false, numeric.plus)
        .asInstanceOf[Accumulator[Any]]
  }

  def newAdder[T](name: String,
                  retainState: Boolean)(implicit numeric: Numeric[T]): Unit = {
    state(name) =
      Accumulator[T](initialValue = numeric.zero, retainState, numeric.plus)
        .asInstanceOf[Accumulator[Any]]
  }

  def newAdder[T](name: String,
                  initialValue: T)(implicit numeric: Numeric[T]): Unit = {
    state(name) = Accumulator[T](initialValue, false, numeric.plus)
      .asInstanceOf[Accumulator[Any]]
  }

  def newAdder[T](name: String, initialValue: T, retainState: Boolean)(
    implicit numeric: Numeric[T]
  ): Unit = {
    state(name) = Accumulator[T](initialValue, retainState, numeric.plus)
      .asInstanceOf[Accumulator[Any]]
  }

  def update(graphState: GraphState): Unit = {
    graphState.state.foreach {
      case (name, value) => {
        state(name) += value.value
      }
    }
  }

  def rotate(): Unit = {
    state.foreach { case (name, accumulator) => accumulator.reset() }
  }

  def apply[T](name: String)(implicit ev: TypeTag[T]): Accumulator[T] = {
    state(name).asInstanceOf[Accumulator[T]]
  }
}

object GraphState {
  def apply() = new GraphState
}
