package com.raphtory.output

import com.google.gson.GsonBuilder
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.time.Interval

import java.io.File

/**
  * `GsonOutputFormat()`
  *   : writes output for Raphtory Job to Gson Format
  */

class GsonOutputFormat(filePath: String) extends OutputFormat {

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

object GsonOutputFormat {

  def apply(filePath: String) =
    new GsonOutputFormat(filePath)
}
