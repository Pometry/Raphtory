package com.raphtory.output

import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm.Row
import com.raphtory.core.config.PulsarController
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema

/**
  * {s}`PulsarOutputFormat(pulsarTopic: String)`
  *   : writes output output to a Raphtory Pulsar topic
  *
  *        {s}`pulsarTopic: String`
  *          : Topic name for writing to Pulsar.
  *
  * Usage while querying or running algorithmic tests:
  *
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
  * ```{code-block} scala
  * import com.raphtory.output.PulsarOutputFormat
  * import com.raphtory.algorithms.generic.EdgeList
  * val outputFormat: PulsarOutputFormat = PulsarOutputFormat("EdgeList")
  * graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
  * ```
  *
  *
  *  ```{seealso}
  *  [](com.raphtory.output.PulsarOutputFormat),
  *  [](com.raphtory.core.components.querymanager.PointQuery), [](com.raphtory.core.components.querymanager.RangeQuery)
  *  ```
  */
class PulsarOutputFormat(val pulsarTopic: String) extends OutputFormat {

  override def write(
      timestamp: Long,
      window: Option[Long],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {}

  def writeToPulsar(
      timestamp: Long,
      window: Option[Long],
      jobID: String,
      row: Row,
      partitionID: Int,
      producer: Producer[String]
  ) = {
    val value = window match {
      case Some(w) => s"$timestamp,$w,${row.getValues().mkString(",")}"
      case None    => s"$timestamp,${row.getValues().mkString(",")}"
    }
    producer.sendAsync(value)
  }

}

object PulsarOutputFormat {
  def apply(pulsarTopic: String) = new PulsarOutputFormat(pulsarTopic)
}
