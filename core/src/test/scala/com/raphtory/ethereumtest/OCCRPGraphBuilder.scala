package com.raphtory.ethereumtest

import com.raphtory.api.input.DoubleProperty
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.LongProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset

class OCCRPGraphBuilder extends GraphBuilder[Array[Byte]] {

  def dateTimeStringToEpoch(s: String): Long =
    LocalDateTime
      .parse(s + " 00-00-00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"))
      .toEpochSecond(ZoneOffset.UTC)

  override def parseTuple(tuple: Array[Byte]): Unit =
    try {
      val line           = new String(tuple)
      if (line contains "payer_name")
        return
      val fileLine       = line.split(";").map(_.trim)
      val source         = fileLine(0)
      val target         = fileLine(9)
      val amountUSD      = fileLine(18).toDouble
      val date           = fileLine(17)
      val timestamp      = dateTimeStringToEpoch(date)
      val srcID          = assignID(source)
      val tarID          = assignID(target)
      val edgeProperties = Properties(
              ImmutableProperty("payer_name", source),
              ImmutableProperty("beneficiary_name", target),
              DoubleProperty("amountUSD", amountUSD),
              LongProperty("timestamp", timestamp)
      )
      addVertex(timestamp, srcID, Properties(ImmutableProperty("name", source)), Type("node"))
      addVertex(timestamp, tarID, Properties(ImmutableProperty("name", target)), Type("node"))
      addEdge(timestamp, srcID, tarID, edgeProperties, Type("transaction"))
    }
    catch { case _: Throwable => }
}
