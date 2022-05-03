package com.raphtory.time

import java.time.Duration
import java.time.Instant
import java.time.Period
import java.time.temporal.TemporalAmount
import scala.language.postfixOps

/** !DoNotDocument */
sealed trait Interval extends Ordered[Interval] {
  def toString: String
  def *(number: Long): Interval
  def /(number: Long): Interval
  protected def toLong: Long

  override def compare(that: Interval): Int =
    toLong - that.toLong toInt
}

case class DiscreteInterval(size: Long) extends Interval {
  def unary_- : Interval                 = DiscreteInterval(-size)
  override def toString: String          = size.toString
  override def *(number: Long): Interval = DiscreteInterval(size * number)
  override def /(number: Long): Interval = DiscreteInterval(size / number)
  override protected def toLong: Long    = size
}

case class TimeInterval(size: TemporalAmount) extends Interval {

  def unary_- : Interval =
    TimeInterval(size match {
      case size: Duration => size.negated
      case size: Period   => size.negated
      case _              => throw new Exception("Unknown interval type")
    })

  override def *(number: Long): Interval =
    TimeInterval(size match {
      case size: Duration => size.multipliedBy(number)
      case size: Period   => size.multipliedBy(number.toInt)
      case _              => throw new Exception("Unknown interval type")
    })

  override def /(number: Long): Interval =
    size match {
      case size: Duration => TimeInterval(size.dividedBy(number))
      case size: Period   => DiscreteInterval(toLong / number)
      case _              => throw new Exception("Unknown interval type")
    }
  override def toString: String          = size.toString
  override protected def toLong: Long    = Instant.ofEpochMilli(0).plus(size).toEpochMilli
}

case object NullInterval extends Interval {
  def unary_- : Interval                 = NullInterval
  override def *(number: Long): Interval = NullInterval
  override def /(number: Long): Interval = NullInterval
  override def toString: String          = "0"
  override protected def toLong: Long    = 0
}
