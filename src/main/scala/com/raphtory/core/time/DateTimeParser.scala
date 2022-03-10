package com.raphtory.core.time

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class DateTimeParser(format: String) {
  val formatter = DateTimeFormatter.ofPattern(format)

  def parse(datetime: String): Long =
    LocalDateTime.parse(datetime, formatter).toInstant(ZoneOffset.UTC).toEpochMilli
}

object DateTimeParser {
  def apply(format: String) = new DateTimeParser(format)
}
