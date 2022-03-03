package com.raphtory.core.time

import com.raphtory.core.time.TimeUtils._
import java.time.temporal.TemporalAmount
import scala.language.postfixOps

sealed trait Interval extends Ordered[Interval] {
  def toString: String
}

case class DiscreteInterval(size: Long) extends Interval {

  override def compare(that: Interval): Int = {
    val agnosticWindow = that.asInstanceOf[DiscreteInterval]
    size - agnosticWindow.size toInt
  }

  override def toString: String = size.toString
}

case class TimeInterval(size: TemporalAmount) extends Interval {

  override def compare(that: Interval): Int = {
    val timeWindow = that.asInstanceOf[TimeInterval]
    temporalAmountToMilli(size) - temporalAmountToMilli(timeWindow.size) toInt
  }

  override def toString: String = size.toString
}
