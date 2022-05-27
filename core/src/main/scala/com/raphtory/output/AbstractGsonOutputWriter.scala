package com.raphtory.output

import com.google.gson.GsonBuilder
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective

abstract class AbstractGsonOutputWriter extends OutputWriter {
  override type OutputType = String

  private val gsonBuilder = new GsonBuilder().setPrettyPrinting().create()

  final override def setupPerspective(perspective: Perspective): Unit = {}

  final override def closePerspective(): Unit = {}

  final override def threadSafeWriteRow(row: Row): Unit = {
    val value = s"${gsonBuilder.toJson(row.getValues())}"
    sink.writeEntity(value)
  }

  final override def close(): Unit = sink.close()
}
