package com.raphtory.output

import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm.Row
import com.raphtory.core.time.Interval
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.File

class FileOutputFormat(filePath: String) extends OutputFormat {

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {
    val dir = new File(s"$filePath/$jobID")

    if (!dir.exists())
      // TODO: Re-enable. Currently throws a NullPointerException
      //logger.debug(s"Output directory '$dir' does not exist. Creating directory...")
      dir.mkdirs()
    else {
      // TODO: Re-enable. Currently throws a NullPointerException
      //logger.warn(s"Output directory '$dir' already exists. Is the Job ID unique?")
    }

    val value = window match {
      case Some(w) => s"$timestamp,$w,${row.getValues().mkString(",")}\n"
      case None    => s"$timestamp,${row.getValues().mkString(",")}\n"
    }

    reflect.io.File(s"$filePath/$jobID/partition-$partitionID").appendAll(value)

    // TODO: Re-enable. Currently throws a NullPointerException
    //logger.info(s"Results successfully written out to directory '$dir'.")
  }
}

object FileOutputFormat {
  def apply(filePath: String) = new FileOutputFormat(filePath)
}
