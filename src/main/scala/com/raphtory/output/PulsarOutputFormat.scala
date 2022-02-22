package com.raphtory.output

import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm.Row
import com.raphtory.core.config.PulsarController
import com.raphtory.core.time.Interval
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema

class PulsarOutputFormat(val pulsarTopic: String) extends OutputFormat {

  override def write(
      timestamp: Long,
      window: Option[Interval],
      jobID: String,
      row: Row,
      partitionID: Int
  ): Unit = {}

  def writeToPulsar(
      timestamp: Long,
      window: Option[Interval],
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
