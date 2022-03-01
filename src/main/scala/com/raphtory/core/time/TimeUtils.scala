package com.raphtory.core.time

import java.time.Instant
import java.time.temporal.TemporalAmount

object TimeUtils {

  val EPOCH_MILLI: Instant = Instant.ofEpochMilli(0)

  implicit class TimeHolder(val time: Long) {

    def -(interval: Interval): Long =
      interval match {
        case AgnosticInterval(size) => time - size
        case TimeInterval(size)     => Instant.ofEpochMilli(time).minus(size).toEpochMilli
      }

    def +(interval: Interval): Long =
      interval match {
        case AgnosticInterval(size) => time + size
        case TimeInterval(size)     => Instant.ofEpochMilli(time).plus(size).toEpochMilli
      }
  }

  def temporalAmountToMilli(amount: TemporalAmount): Long =
    EPOCH_MILLI.plus(amount).toEpochMilli
}
