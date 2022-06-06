package com.raphtory.time

import com.raphtory.internals.time.IntervalParser
import com.raphtory.internals.time.InvalidIntervalException
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration
import java.time.Period

class IntervalParserTest extends AnyFunSuite {
  test("Duration with days is correctly parsed") {
    val interval = IntervalParser.parse("2 days and 1 hour")
    assert(interval.size === Duration.ofDays(2).plus(Duration.ofHours(1)))
  }

  test("Duration without days is correctly parsed") {
    val interval = IntervalParser.parse("3 hours, 4 minutes, 45 seconds, and 670 millis")
    assert(
            interval.size === Duration
              .ofHours(3)
              .plus(Duration.ofMinutes(4))
              .plus(Duration.ofSeconds(45))
              .plus(Duration.ofMillis(670))
    )
  }

  test("Period is correctly parsed") {
    val interval = IntervalParser.parse("10 years 2 months 1 week and 3 days")
    assert(
            interval.size === Period
              .ofYears(10)
              .plus(Period.ofMonths(2))
              .plus(Period.ofWeeks(1))
              .plus(Period.ofDays(3))
    )
  }

  test("Unknown units cause an exception") {
    assertThrows[InvalidIntervalException] {
      IntervalParser.parse("3 weeeks")
    }
  }
}
