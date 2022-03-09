package com.raphtory.core.time

import com.typesafe.config.Config

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class DateTimeParser(conf: Config) {

  def parse(datetime: String): Long = {
    val format    = conf.getString("raphtory.time.format")
    val formatter = DateTimeFormatter.ofPattern(format)
    LocalDateTime.parse(datetime, formatter).toInstant(ZoneOffset.UTC).toEpochMilli
  }
}

object DateTimeParser {
  def apply(conf: Config) = new DateTimeParser(conf)
}
