package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.PulsarSink
import com.typesafe.config.Config

/** Writes the rows of a `Table` to the Pulsar topic specified by `pulsarTopic` in CSV format.
  * @param pulsarTopic name of the pulsar topic
  *
  * Usage:
  * (while querying or running algorithmic tests)
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.algorithms.api.OutputFormat
  * import com.raphtory.components.spout.instance.ResourceSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(ResourceSpout("resource"), graphBuilder)
  * val testDir = "/tmp/raphtoryTest"
  * val outputFormat: OutputFormat = PulsarOutputFormat("edge-list-topic")
  *
  * graph.execute(EdgeList()).writeTo(outputFormat)
  * }}}
  *
  *  @see [[com.raphtory.algorithms.api.OutputFormat]]
  *       [[com.raphtory.client.GraphDeployment]]
  *       [[com.raphtory.deployment.Raphtory]]
  */
case class PulsarOutputFormat(pulsarTopic: String) extends OutputFormat {

  class PulsarOutputWriter(val pulsarTopic: String, config: Config)
          extends AbstractCsvOutputWriter {
    override protected def createSink() = new PulsarSink(pulsarTopic, config)
  }

  override def outputWriter(jobID: String, partitionID: Int, conf: Config): OutputWriter =
    new PulsarOutputWriter(pulsarTopic, conf)
}
