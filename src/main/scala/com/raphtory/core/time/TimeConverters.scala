package com.raphtory.core.time

import java.time.Instant
import java.time.ZoneOffset

object TimeConverters {

  implicit class TimeHolder(val time: Long) {

    def -(interval: Interval): Long =
      interval match {
        case DiscreteInterval(size) => time - size
        case TimeInterval(size)     =>
          Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC).minus(size).toInstant.toEpochMilli
      }

    def +(interval: Interval): Long =
      interval match {
        case DiscreteInterval(size) => time + size
        case TimeInterval(size)     =>
          Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC).plus(size).toInstant.toEpochMilli
      }
  }
}
