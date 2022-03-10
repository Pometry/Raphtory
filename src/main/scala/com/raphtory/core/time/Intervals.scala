package com.raphtory.core.time

import java.time.Instant
import java.time.temporal.TemporalAmount
import scala.language.postfixOps

sealed trait Interval extends Ordered[Interval] {
  def toString: String
  protected def toLong: Long

  override def compare(that: Interval): Int =
    toLong - that.toLong toInt
}

case class DiscreteInterval(size: Long) extends Interval {
  override protected def toLong: Long = size
  override def toString: String       = size.toString
}

case class TimeInterval(size: TemporalAmount) extends Interval {
  override protected def toLong: Long = Instant.ofEpochMilli(0).plus(size).toEpochMilli
  override def toString: String       = size.toString
}
