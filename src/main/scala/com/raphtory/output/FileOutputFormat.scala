package com.raphtory.output

import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm.Row
import com.raphtory.core.time.Interval
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.File

/**
  * {s}`FileOutputFormat(filePath: String)`
  *   : writes output for Raphtory Job and Partition for a pre-defined window and timestamp to File
  *
  *     {s}`filePath: String`
  *       : Filepath for writing Raphtory output.
  *
  * Usage while querying or running algorithmic tests:
  *
  * ```{code-block} scala
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.core.algorithm.OutputFormat
  * import com.raphtory.core.components.graphbuilder.GraphBuilder
  * import com.raphtory.core.components.spout.Spout
  *
  * val graph = Raphtory.createTypedGraph[T](Spout[T], GraphBuilder[T])
  * val testDir = "/tmp/raphtoryTest"
  * val outputFormat: OutputFormat = FileOutputFormat(testDir)
  *
  * graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.core.algorithm.OutputFormat),
  *  [](com.raphtory.core.client.RaphtoryClient),
  *  [](com.raphtory.core.client.RaphtoryGraph),
  *  [](com.raphtory.core.deploy.Raphtory)
  *  ```
  */
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
