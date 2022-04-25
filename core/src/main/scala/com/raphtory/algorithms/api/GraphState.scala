package com.raphtory.algorithms.api

/**
  * Public interface for global accumulators
  *
  *  The GraphState tracks global (graph-level) variables during algorithm execution.
  *  Graph-level state takes the form of accumulators which expose the value computed during the last step/iteration
  *  and allow accumulation of new state based on a reduction function.
  *
  * @see [[com.raphtory.algorithms.api.GraphPerspective]], [[com.raphtory.algorithms.api.Accumulator]]
  */
abstract class GraphState {

  /**  Create a new general Accumulator with value type `T` under name `name`
    *
    * @param name Reference name of accumulator
    * @param initialValue Initial value for accumulator
    * @param retainState If retainState = true, accumulation for the next step/iteration of an algorithm continues with the
    *                    previously computed value, otherwise, the value is reset to initialValue before each step.
    *
    * @param op Reduction function for the accumulator.
    */
  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit

  def newConstant[T](
                      name: String,
                      value: T
                    ): Unit

  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String): Unit
  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, initialValue: T): Unit
  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, retainState: Boolean): Unit
  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String): Unit
  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String, initialValue: T): Unit
  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String, retainState: Boolean): Unit
  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit

  /** Create a new histogram that tracks the distribution of a graph quantity */
  def newHistogram(
      name: String,
      noBins: Int,
      retainState: Boolean
  ): Unit

  def newAll(name: String, retainState: Boolean = false): Unit

  def newAny(name: String, retainState: Boolean = false): Unit

  /** Return the accumulator stored under name `name` */
  def apply[T](name: String): Accumulator[T]

  /** Return the accumulator stored under name name if it exists, else return None */
  def get[T](name: String): Option[Accumulator[T]]

  /** Check if graph state with `name` exists */
  def contains(name: String): Boolean
}
