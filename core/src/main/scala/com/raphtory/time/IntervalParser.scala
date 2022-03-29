package com.raphtory.time

import java.time.Duration
import java.time.Period
import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object IntervalParser {

  sealed trait Unit
  sealed trait PeriodUnit   extends Unit // Can only be handled by java.time.Period
  sealed trait DurationUnit extends Unit // Can only be handled by java.time.Duration
  case object YEARS         extends PeriodUnit
  case object MONTHS        extends PeriodUnit
  case object WEEKS         extends PeriodUnit
  case object DAYS          extends PeriodUnit with DurationUnit
  case object HOURS         extends DurationUnit
  case object MINUTES       extends DurationUnit
  case object SECONDS       extends DurationUnit
  case object MILLIS        extends DurationUnit

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

    lazy val periodFromAmounts   = amounts map {
      case (number, unit) => periodFromUnit(number, unit.asInstanceOf[PeriodUnit])
    } reduce (_.plus(_))
    lazy val durationFromAmounts = amounts map {
      case (number, unit) => durationFromUnit(number, unit.asInstanceOf[DurationUnit])
    } reduce (_.plus(_))
    val temporalAmount           = Try(periodFromAmounts).orElse(Try(durationFromAmounts))

    temporalAmount match {
      case Success(temporalAmount) => TimeInterval(temporalAmount)
      case Failure(_)              =>
        val msg = s"You cannot set years, months, or weeks at the same time as" +
          s" hours, minutes, seconds, or milliseconds: '$interval'"
        throw new InvalidIntervalException(msg)
    }
  }

  private val periodFromUnit = (number: Int, unit: PeriodUnit) =>
    unit match {
      case YEARS  => Period.ofYears(number)
      case MONTHS => Period.ofMonths(number)
      case WEEKS  => Period.ofWeeks(number)
      case DAYS   => Period.ofDays(number)
    }

  private val durationFromUnit = (number: Int, unit: DurationUnit) =>
    unit match {
      case DAYS    => Duration.ofDays(number)
      case HOURS   => Duration.ofHours(number)
      case MINUTES => Duration.ofMinutes(number)
      case SECONDS => Duration.ofSeconds(number)
      case MILLIS  => Duration.ofMillis(number)
    }

  private def translateUnit(unit: String): Unit =
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
