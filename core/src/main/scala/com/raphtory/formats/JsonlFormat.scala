package com.raphtory.formats

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector

case class JsonlFormat() extends Format[String] {

  private var gsonBuilder: Gson = _

  override def open(connector: SinkConnector[String]): Unit =
    gsonBuilder = new GsonBuilder().create()

  override def setupPerspective(
      connector: SinkConnector[String],
      perspective: Perspective
  ): Unit = {}

  override def closePerspective(connector: SinkConnector[String]): Unit = {}

  override def writeRow(connector: SinkConnector[String], row: Row): Unit = {
    val value = s"${gsonBuilder.toJson(row.getValues())}"
    connector.write(value)
  }

  override def close(connector: SinkConnector[String]): Unit = {}
}
