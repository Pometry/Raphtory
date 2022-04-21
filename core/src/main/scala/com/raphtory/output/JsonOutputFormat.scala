package com.raphtory.output

import com.google.gson.GsonBuilder
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.time.Interval

class JsonOutputFormat(filePath: String) extends OutputFormat {

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {
    val gsonBuilder = new GsonBuilder()
      .setPrettyPrinting()
      .create()
    val jsonString  = gsonBuilder.toJson(row.getValues())
    println(jsonString)
  }
}

object JsonOutputFormat {

  def apply(filePath: String) =
    new JsonOutputFormat(filePath)
}
