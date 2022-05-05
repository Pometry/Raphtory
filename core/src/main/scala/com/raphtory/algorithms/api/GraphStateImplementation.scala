package com.raphtory.algorithms.api

import scala.collection.mutable

/**
  * @note DoNotDocument
  */
private class AccumulatorImplementation[T](
    initialValue: T,
    retainState: Boolean = false,
    op: (T, T) => T
) extends Accumulator[T] {
  var currentValue: T = initialValue
  var value: T        = initialValue

  def +=(newValue: T): Unit =
    this.synchronized(this.currentValue = op(currentValue, newValue))

  def reset(): Unit = {
    if (retainState)
      value = op(value, currentValue)
    else
      value = currentValue
    currentValue = initialValue
  }
}

/**
  * @note DoNotDocument
  */
private object AccumulatorImplementation {

  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new AccumulatorImplementation[T](initialValue, retainState, op)
}

class Bounded[T](min: T, max: T) {
  def MIN: T = min
  def MAX: T = max
}

/**
  * @note DoNotDocument
  */
object Bounded {
  def apply[T](min: T, max: T) = new Bounded[T](min, max)

  implicit val intBounds: Bounded[Int]   = Bounded(Int.MinValue, Int.MaxValue)
  implicit val longBounds: Bounded[Long] = Bounded(Long.MinValue, Long.MaxValue)

  implicit val doubleBounds: Bounded[Double] =
    Bounded(Double.NegativeInfinity, Double.PositiveInfinity)

  implicit val floatBounds: Bounded[Float] =
    Bounded(Float.NegativeInfinity, Float.PositiveInfinity)
}

/**
  * @note DoNotDocument
  */
class GraphStateImplementation extends GraphState {
  private val state = mutable.Map.empty[String, AccumulatorImplementation[Any]]

  /** @inheritdoc */
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
    state(name) = AccumulatorImplementation[T](initialValue, retainState = false, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String)(implicit numeric: Numeric[T]): Unit =
    state(name) = AccumulatorImplementation[T](numeric.one, retainState = false, op = numeric.times)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String, initialValue: T)(implicit numeric: Numeric[T]): Unit =
    state(name) =
      AccumulatorImplementation[T](initialValue, retainState = false, op = numeric.times)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](numeric.one, retainState, numeric.times)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.times)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit =
    state(name) = AccumulatorImplementation[T](bounded.MIN, retainState = false, numeric.max)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](bounded.MIN, retainState = retainState, numeric.max)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState = false, numeric.max)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState = retainState, numeric.max)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit =
    state(name) = AccumulatorImplementation[T](bounded.MAX, retainState = false, numeric.min)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](bounded.MAX, retainState = retainState, numeric.min)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState = false, numeric.min)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    state(name) = AccumulatorImplementation[T](initialValue, retainState = retainState, numeric.min)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newAll(name: String, retainState: Boolean): Unit =
    state(name) = AccumulatorImplementation[Boolean](true, retainState, _ && _)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newAny(name: String, retainState: Boolean): Unit =
    state(name) = AccumulatorImplementation[Boolean](initialValue = false, retainState, _ || _)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def update(graphState: GraphStateImplementation): Unit =
    graphState.state.foreach {
      case (name, value) =>
        state(name) += value.currentValue
    }

  def rotate(): Unit                         =
    state.foreach { case (_, accumulator) => accumulator.reset() }

  def apply[T](name: String): Accumulator[T] =
    state(name).asInstanceOf[Accumulator[T]]

  /** @inheritdoc */
  override def get[T](name: String): Option[Accumulator[T]] =
    state.get(name).asInstanceOf[Option[Accumulator[T]]]

  /** @inheritdoc */
  override def contains(name: String): Boolean =
    state.contains(name)

}

object GraphStateImplementation {
  def apply() = new GraphStateImplementation
  val empty   = new GraphStateImplementation
}
