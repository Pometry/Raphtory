package com.raphtory.output

import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm.Row
import com.raphtory.core.config.PulsarController
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema

/**
  * # PulsarOutputFormat
  * `PulsarOutputFormat` writes output to a Raphtory Pulsar topic
  *
  * Usage while querying or running algorithmic tests:
  *
  * ```{code-block} scala
  * import com.raphtory.output.PulsarOutputFormat
  * val outputFormat: PulsarOutputFormat = PulsarOutputFormat("EdgeList" + deploymentID)
  *     val queryProgressTracker =
  *     graph.rangeQuery(
  *           graphAlgorithm = EdgeList(),
  *           outputFormat = outputFormat,
  *           start = 1,
  *           end = 32674,
  *           increment = 10000,
  *           windows = List(500, 1000, 10000)
  *     )
  * ```
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
