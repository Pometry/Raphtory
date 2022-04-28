package com.raphtory.output

import com.google.gson.GsonBuilder
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.time.Interval

import java.io.File

/**
  * {s}`JsonOutputFormat()`
  *   : writes output for Raphtory Job to Json Format
  */

class JsonOutputFormat(filePath: String) extends OutputFormat {

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {
    val dir = new File(s"$filePath/$jobID")
    dir.mkdirs()

    val gsonBuilder = new GsonBuilder()
      .setPrettyPrinting()
      .create()

    val value = s"${gsonBuilder.toJson(row.getValues())}"
    reflect.io.File(s"$filePath/$jobID/partition-$partitionID").appendAll(value)
  }
}

object JsonOutputFormat {

  def apply(filePath: String) =
    new JsonOutputFormat(filePath)
}
