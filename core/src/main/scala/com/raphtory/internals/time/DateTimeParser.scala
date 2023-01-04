package com.raphtory.internals.time

import com.raphtory.internals.time.IntervalParser.MILLIS

import java.text.DateFormat
import java.text.SimpleDateFormat
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

  def parse(epoch: Long): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    dateFormat.format(new java.util.Date(epoch))
  }

}

private[raphtory] object DateTimeParser {
  def apply(format: String = "yyyy[-MM[-dd[ HH[:mm[:ss[.SSS]]]]]]") = new DateTimeParser(format)
}
