package com.raphtory.api.time

import java.time.temporal.TemporalAmount
import java.time.Duration
import java.time.Instant
import java.time.Period
import scala.language.postfixOps

/** Base trait for classes representing an interval.
  *
  * An interval is the space between two points in time.
  * The dimension of this time space might be expressed in terms of the the actual one or using an alternative scale,
  * but `toString` must return a representation in accordance.
  *
  * Implementations of this trait must override `ouput`, `*`, and `/` methods.
  */
sealed trait Interval extends Ordered[Interval] {

  /** Returns a `String` representation of this `Interval`. */
  override def toString: String = output.toString

  /** Returns the best output format for this `Interval`. */
  def output: Any

  /** Returns the `Interval` resulting from multiplying this `Interval` by `number`. */
  def *(number: Long): Interval

  /** Returns the `Interval` resulting from dividing this `Interval` by `number` (might be an approximation). */
  def /(number: Long): Interval

  protected def toLong: Long

  override def compare(that: Interval): Int =
    toLong - that.toLong toInt
}

case class DiscreteInterval(size: Long) extends Interval {
  def unary_- : Interval                 = DiscreteInterval(-size)
  override def *(number: Long): Interval = DiscreteInterval(size * number)
  override def /(number: Long): Interval = DiscreteInterval(size / number)
  override protected def toLong: Long    = size
  override def output: Any               = size
}

case class TimeInterval(size: TemporalAmount, name: String) extends Interval {

  def unary_- : Interval =
    TimeInterval(
            size match {
              case size: Duration => size.negated
              case size: Period   => size.negated
              case _              => throw new Exception("Unknown interval type")
            },
            name
    )

  override def *(number: Long): Interval =
    TimeInterval(
            size match {
              case size: Duration => size.multipliedBy(number)
              case size: Period   => size.multipliedBy(number.toInt)
              case _              => throw new Exception("Unknown interval type")
            },
            name
    )

  override def /(number: Long): Interval =
    size match {
      case size: Duration => TimeInterval(size.dividedBy(number), name)
      case size: Period   => DiscreteInterval(toLong / number)
      case _              => throw new Exception("Unknown interval type")
    }
  override protected def toLong: Long    = Instant.ofEpochMilli(0).plus(size).toEpochMilli
  override def output: Any               = name
}

case object NullInterval extends Interval {
  def unary_- : Interval                 = NullInterval
  override def *(number: Long): Interval = NullInterval
  override def /(number: Long): Interval = NullInterval
  override protected def toLong: Long    = 0
  override def output: Any               = 0
}
