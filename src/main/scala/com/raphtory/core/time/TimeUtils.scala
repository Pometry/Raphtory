package com.raphtory.core.time

import java.time.Duration
import java.time.Instant
import java.time.Period
import java.time.temporal.TemporalAmount
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object TimeUtils {

  val EPOCH_MILLI: Instant = Instant.ofEpochMilli(0)

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

  def temporalAmountToMilli(amount: TemporalAmount): Long =
    EPOCH_MILLI.plus(amount).toEpochMilli

  def parseInterval(interval: String): Interval = {
    val isoInterval = tryTransformationToIso8601(interval)

    val intervalTry: Try[Interval] = Try(Period.parse(isoInterval))
      .orElse(Try(Duration.parse(isoInterval)))
      .map(TimeInterval(_))

    intervalTry match {
      case Success(interval) => interval
      case Failure(_)        => throw new Exception(s"Failed to parse interval '$interval'")
    }
  }

  private def tryTransformationToIso8601(interval: String) = {
    val singleCharacters = interval
      .replaceAll("\\s", "")
      .toUpperCase()
      .replaceAll("AND", "")
      .replaceAll("YEARS?", "Y")
      .replaceAll("MONTHS?", "M")
      .replaceAll("WEEKS?", "W")
      .replaceAll("DAYS?", "D")
      .replaceAll("HOURS?", "H")
      .replaceAll("MINUTES?", "M")
      .replaceAll("SECONDS?", "S")

    if (singleCharacters.charAt(singleCharacters.length - 1) == 'D')
      "P" + singleCharacters
    else if (singleCharacters.contains('D'))
      "P" + singleCharacters.replaceFirst("D", "DT")
    else
      "PT" + singleCharacters
  }
}
