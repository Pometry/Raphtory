package com.raphtory.core.algorithm

import scala.reflect.runtime.universe._

abstract class Accumulator[T] {
  def value: T

  def +=(newValue: T): Unit
}

/**
  *  {s}`GraphState`
  *    : Public {s}`GraphState` interface for global accumulators
  *
  *  The {s}`GraphState` tracks global (graph-level) variables during algorithm execution.
  *  Graph-level state takes the form of accumulators which expose the value computed during the last step/iteration
  *  and allow accumulation of new state based on a reduction function.
  *
  *  ## Methods
  *
  *    {s}`newAccumulator[T](name: String, initialValue: T, retainState: Boolean = false, op: (T, T) => T): Unit`
  *      : Create a new general Accumulator with value type `T` under name `name`.
  *
  *        {s}`name: String`
  *          : Reference name of accumulator
  *
  *        {s}`initialValue: T`
  *          : Initial value for accumulator
  *
  *        {s}`retainState: Boolean = false`
  *          : If {s}`retainState = true`, accumulation for the next step/iteration of an algorithm continues with the
  *            previously computed value, otherwise, the value is reset to `initialValue` before each step.
  *
  *        {s}`op: (T, T) => T`
  *          : Reduction function for the accumulator.
  *
  *    {s}`newAdder[T: Numeric](name: String, initialValue: T = 0, retainState: Boolean = false)`
  *      : Create a new accumulator that sums values.
  *
  *    {s}`newMultiplier[T: Numeric](name: String, initialValue: T = 1, retainState: Boolean = false)`
  *      : Create a new accumulator that multiplies values.
  *
  *    {s}`apply[T](name: String): Accumulator[T]`
  *      : Return the accumulator stored under name `name`.
  *
  *    {s}`get[T](name: String): Option[Accumulator[T]]`
  *      : Return the accumulator stored under name `name` if it exists, else return {s}`None`
  *
  *    {s}`contains(name: String): Boolean`
  *      : Check if graph state with `name` exists
  *
  *  # Accumulator
  *
  *  {s}`Accumulator[T]`
  *    : Public accumulator interface
  *
  *  ## Methods
  *
  *    {s}`+= (newValue: T): Unit`
  *      : add new value to accumulator
  *
  *    {s}`value: T`
  *      : get last accumulated value
  */
abstract class GraphState {

  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit

  def newAdder[T: Numeric](name: String): Unit
  def newAdder[T: Numeric](name: String, initialValue: T): Unit
  def newAdder[T: Numeric](name: String, retainState: Boolean): Unit
  def newAdder[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  def newMultiplier[T: Numeric](name: String): Unit
  def newMultiplier[T: Numeric](name: String, initialValue: T): Unit
  def newMultiplier[T: Numeric](name: String, retainState: Boolean): Unit
  def newMultiplier[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  def newMax[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit

  def newMax[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  def newMax[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  def newMax[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  def newMin[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit

  def newMin[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  def newMin[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  def newMin[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  def apply[T](name: String): Accumulator[T]
  def get[T](name: String): Option[Accumulator[T]]
  def contains(name: String): Boolean
}
