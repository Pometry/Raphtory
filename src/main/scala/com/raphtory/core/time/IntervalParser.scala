package com.raphtory.core.time

import java.time.Duration
import java.time.Period
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.YEARS
import java.time.temporal.ChronoUnit.MONTHS
import java.time.temporal.ChronoUnit.WEEKS
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.ChronoUnit.HOURS
import java.time.temporal.ChronoUnit.MINUTES
import java.time.temporal.ChronoUnit.SECONDS
import java.time.temporal.ChronoUnit.MILLIS
import scala.language.postfixOps

object IntervalParser {

  private val periodOnlyUnits   = List(YEARS, MONTHS, WEEKS)
  private val durationOnlyUnits = List(HOURS, MINUTES, SECONDS, MILLIS)

  def parse(interval: String): TimeInterval = {
    val cleaned     = interval
      .trim()
      .replaceAll(", ", " ")
      .replaceAll(" and ", " ")
    val stringPairs = cleaned.split("\\s+").grouped(2).toList

    val amounts = stringPairs map {
      case Array(numberStr, unitStr) =>
        val number =
          try numberStr.toInt
          catch {
            case _: NumberFormatException =>
              val msg = s"Unable to parse number '$numberStr' in pair ($numberStr, $unitStr)"
              throw new InvalidIntervalException(msg)
          }
        val last   = unitStr.length - 1
        val unit   =
          if (number == 1)
            translateUnit(unitStr)
          else if (unitStr(last) == 's')
            translateUnit(unitStr.substring(0, last))
          else {
            val msg = s"Number of '$unitStr' is not equal to 1 so needs to end with 's'"
            throw new InvalidIntervalException(msg)
          }
        (number, unit)
    }

    val units                 = amounts map { case (number, unit) => unit }
    val periodRepresentable   = units intersect periodOnlyUnits nonEmpty
    val durationRepresentable = units intersect durationOnlyUnits nonEmpty

    if (periodRepresentable && durationRepresentable) {
      val msg = s"You cannot set years, months, or weeks at the same time as" +
        s" hours, minutes, seconds, or milliseconds: '$interval'"
      throw new InvalidIntervalException(msg)
    }

    val temporalAmount =
      if (periodRepresentable)
        amounts.foldLeft(Period.ZERO)({
          case (accumulator, (number, unit)) => accumulator.plus(periodFromUnit(number, unit))
        })
      else
        amounts.foldLeft(Duration.ZERO)({
          case (accumulator, (number, unit)) => accumulator.plus(durationFromUnit(number, unit))
        })

    TimeInterval(temporalAmount)
  }

  private val periodFromUnit = (number: Int, unit: ChronoUnit) =>
    unit match {
      case YEARS  => Period.ofYears(number)
      case MONTHS => Period.ofMonths(number)
      case WEEKS  => Period.ofWeeks(number)
      case DAYS   => Period.ofDays(number)
    }

  private val durationFromUnit = (number: Int, unit: ChronoUnit) =>
    unit match {
      case DAYS    => Duration.ofDays(number)
      case HOURS   => Duration.ofHours(number)
      case MINUTES => Duration.ofMinutes(number)
      case SECONDS => Duration.ofSeconds(number)
      case MILLIS  => Duration.ofMillis(number)
    }

  private def translateUnit(unit: String): ChronoUnit =
    unit match {
      case "year"        => YEARS
      case "month"       => MONTHS
      case "week"        => WEEKS
      case "day"         => DAYS
      case "hour"        => HOURS
      case "minute"      => MINUTES
      case "min"         => MINUTES
      case "second"      => SECONDS
      case "sec"         => SECONDS
      case "milli"       => MILLIS
      case "millisecond" => MILLIS
      case _             => throw new InvalidIntervalException(s"Invalid unit '$unit'")
    }
}
