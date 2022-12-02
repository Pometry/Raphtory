package com.raphtory.examples.ethereumtest

import com.raphtory.api.input.MutableDouble
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset

object OCCRPGraphBuilder {

  def dateTimeStringToEpoch(s: String): Long =
    LocalDateTime
      .parse(s + " 00-00-00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss"))
      .toEpochSecond(ZoneOffset.UTC)

  def parse(graph: Graph, tuple: Array[Byte]): Unit =
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
              ImmutableString("payer_name", source),
              ImmutableString("beneficiary_name", target),
              MutableDouble("amountUSD", amountUSD),
              MutableLong("timestamp", timestamp)
      )
      graph.addVertex(timestamp, srcID, Properties(ImmutableString("name", source)), Type("node"))
      graph.addVertex(timestamp, tarID, Properties(ImmutableString("name", target)), Type("node"))
      graph.addEdge(timestamp, srcID, tarID, edgeProperties, Type("transaction"))
    }
    catch { case _: Throwable => }
}
