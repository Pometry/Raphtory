package com.raphtory.output

import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm.Row
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.File

/**
  * {s}`FileOutputFormat(filePath: String)`
  *
  *   : writes output for Raphtory Job and Partition for a pre-defined window and timestamp to File
  *        {s}`filePath: String`
  *          : Filepath for writing Raphtory output.
  *
  *  ## Methods
  *    {s}`write[T](timestamp: Long, window: Option[Long], jobID: String, row: Row, partitionID: Int): Unit`
  *      : Writes computed row for a partition for a specific window frame
  *
  *        {s}`timestamp: Long`
  *          : Timestamp for the write operation.
  *
  *        {s}`window: Option[Long]`
  *          : Window of start and end timestamps for which this row is computed.
  *
  *        {s}`jobID: String`
  *          : Job identifier for Raphtory job.
  *
  *        {s}`row: Row`
  *          : Row for computation.
  *
  *        {s}`partitionID: Int``
  *          : Paritition identifier.
  *
  * Usage while querying or running algorithmic tests:
  *
  * ```{code-block} scala
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.algorithms.generic.centrality.WeightedDegree
  * val testDir  =  "/tmp/raphtoryTest"
  * val defaultOutputFormat: OutputFormat  =  FileOutputFormat(testDir)
  * algorithmTest(WeightedDegree(), outputFormat, 1, 32674, 10000, List(500, 1000, 10000))
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.output.FileOutputFormat)
  *  [](com.raphtory.core.components.querymanager.PointQuery), [](com.raphtory.core.components.querymanager.RangeQuery)
  *  ```
  */
class FileOutputFormat(filePath: String) extends OutputFormat {

  override def write(
      timestamp: Long,
      window: Option[Long],
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
