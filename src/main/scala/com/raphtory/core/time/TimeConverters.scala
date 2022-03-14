package com.raphtory.core.time

import java.time.Instant

object TimeConverters {

  implicit class TimeHolder(val time: Long) {

    def -(interval: Interval): Long =
      interval match {
        case DiscreteInterval(size) => time - size
        case TimeInterval(size)     => Instant.ofEpochMilli(time).minus(size).toEpochMilli
      }

    def +(interval: Interval): Long =
      interval match {
        case DiscreteInterval(size) => time + size
        case TimeInterval(size)     => Instant.ofEpochMilli(time).plus(size).toEpochMilli
      }
  }
}
