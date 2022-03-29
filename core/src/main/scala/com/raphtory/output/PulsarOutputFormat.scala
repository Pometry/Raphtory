package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.Row
import com.raphtory.config.PulsarController
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema

/**
  * {s}`PulsarOutputFormat(pulsarTopic: String)`
  *   : writes output output to a Raphtory Pulsar topic
  *
  *     {s}`pulsarTopic: String`
  *       : Topic name for writing to Pulsar.
  *
  * Usage while querying or running algorithmic tests:
  *
  * ```{code-block} scala
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.PulsarOutputFormat
  * import com.raphtory.algorithm.api.OutputFormat
  * import com.raphtory.components.graphbuilder.GraphBuilder
  * import com.raphtory.components.spout.Spout
  *
  * val graph = Raphtory.createGraph[T](Spout[T], GraphBuilder[T])
  * val outputFormat: OutputFormat = PulsarOutputFormat("EdgeList")
  *
  * graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.algorithms.api.OutputFormat),
  *  [](com.raphtory.client.RaphtoryClient),
  *  [](com.raphtory.client.RaphtoryGraph),
  *  [](com.raphtory.deploy.Raphtory)
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
