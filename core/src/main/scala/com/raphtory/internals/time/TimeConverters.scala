package com.raphtory.internals.time

import com.raphtory.api.time.DiscreteInterval
import com.raphtory.api.time.Interval
import com.raphtory.api.time.NullInterval
import com.raphtory.api.time.TimeInterval

import java.time.Instant
import java.time.ZoneOffset

private[raphtory] object TimeConverters {

  implicit class TimeHolder(val time: Long) {

    def -(interval: Interval): Long =
      interval match {
        case NullInterval           => time
        case DiscreteInterval(size) => time - size
        case TimeInterval(size)     =>
          Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC).minus(size).toInstant.toEpochMilli
      }

    def +(interval: Interval): Long =
      interval match {
        case NullInterval           => time
        case DiscreteInterval(size) => time + size
        case TimeInterval(size)     =>
          Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC).plus(size).toInstant.toEpochMilli
      }
  }
}
