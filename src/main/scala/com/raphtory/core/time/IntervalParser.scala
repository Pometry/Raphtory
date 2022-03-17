package com.raphtory.core.time

import java.time.Duration
import java.time.Period
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object IntervalParser {

  def parse(interval: String): TimeInterval = {
    val isoInterval = tryTransformationToIso8601(interval)
    val intervalTry = Try(Period.parse(isoInterval))
      .orElse(Try(Duration.parse(isoInterval)))
      .map(TimeInterval(_))
    intervalTry match {
      case Success(interval)  => interval
      case Failure(exception) =>
        throw new InvalidIntervalException(
                s"Failed to parse interval '$interval'",
                exception.getCause
        )
    }
  }

  private def tryTransformationToIso8601(interval: String) = {
    val singleCharacters = interval
      .replaceAll("\\s", "")
      .replaceAll(",", "")
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
