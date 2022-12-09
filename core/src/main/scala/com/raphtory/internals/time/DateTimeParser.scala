package com.raphtory.internals.time

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.util.Try

private[raphtory] class DateTimeParser(format: String) {
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)

  def parse(datetime: String): Long =
    Try(LocalDateTime.parse(datetime, formatter))
      .orElse(Try(LocalDate.parse(datetime, formatter).atTime(LocalTime.of(0, 0))))
      .map(_.toInstant(ZoneOffset.UTC).toEpochMilli)
      .get
}

private[raphtory] object DateTimeParser {
  def apply(format: String = "yyyy-MM-dd[ HH:mm:ss[.SSS]]") = new DateTimeParser(format)
}
