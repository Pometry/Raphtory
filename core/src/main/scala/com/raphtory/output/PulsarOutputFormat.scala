package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.PulsarSink
import com.typesafe.config.Config

case class PulsarOutputFormat(pulsarTopic: String) extends OutputFormat {

  override def outputWriter(jobID: String, partitionID: Int, conf: Config): OutputWriter =
    new PulsarOutputWriter(pulsarTopic, conf)
}

/** Writes output output to a Raphtory Pulsar topic
  * @param pulsarTopic Topic name for writing to Pulsar.
  *
  * Usage:
  * (while querying or running algorithmic tests)
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.PulsarOutputFormat
  * import com.raphtory.algorithms.api.OutputFormat
  * import com.raphtory.components.graphbuilder.GraphBuilder
  * import com.raphtory.components.spout.Spout
  *
  * val graph = Raphtory.createGraph[T](Spout[T], GraphBuilder[T])
  * val outputFormat: OutputFormat = PulsarOutputFormat("EdgeList")
  *
  * graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
  * }}}
  *
  *  @see [[com.raphtory.algorithms.api.OutputFormat]]
  *       [[com.raphtory.client.GraphDeployment]]
  *       [[com.raphtory.deployment.Raphtory]]
  */
class PulsarOutputWriter(val pulsarTopic: String, config: Config) extends AbstractCsvOutputWriter {
  override protected def createSink() = new PulsarSink(pulsarTopic, config)
}

/** Writes output output to a Raphtory Pulsar topic */
object PulsarOutputFormat {

  /** @param pulsarTopic Topic name for writing to Pulsar. */
  def apply(pulsarTopic: String) = new PulsarOutputFormat(pulsarTopic)
}
