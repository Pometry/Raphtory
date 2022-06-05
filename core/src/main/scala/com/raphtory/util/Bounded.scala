package com.raphtory.util

class Bounded[T](min: T, max: T) {
  def MIN: T = min
  def MAX: T = max
}

/**
  * Implicit minimum and maximum possible values for numeric types.
  *
  * This is used to provide a unified interface for getting default values for `min` and `max` accumulators.
  */
object Bounded {
  implicit val intBounds: Bounded[Int]   = Bounded(Int.MinValue, Int.MaxValue)
  implicit val longBounds: Bounded[Long] = Bounded(Long.MinValue, Long.MaxValue)

  implicit val doubleBounds: Bounded[Double] =
    Bounded(Double.NegativeInfinity, Double.PositiveInfinity)

  implicit val floatBounds: Bounded[Float]    =
    Bounded(Float.NegativeInfinity, Float.PositiveInfinity)
  def MAX[T](implicit bounded: Bounded[T]): T = bounded.MAX
  def MIN[T](implicit bounded: Bounded[T]): T = bounded.MIN
  def apply[T](min: T, max: T)                = new Bounded[T](min, max)
}
