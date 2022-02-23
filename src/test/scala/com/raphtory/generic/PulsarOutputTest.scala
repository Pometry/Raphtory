package com.raphtory.generic

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.config.PulsarController
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.output.PulsarOutputFormat
import org.apache.pulsar.client.api.Schema

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

class PulsarOutputTest extends BaseRaphtoryAlgoTest[String] {
  val outputFormat: PulsarOutputFormat = PulsarOutputFormat("ConnectedComponents" + deploymentID)

  override def setSpout(): Spout[String]               = FileSpout(s"/tmp/lotr.csv")
  override def setGraphBuilder(): GraphBuilder[String] = new LOTRGraphBuilder()

  override def setup() =
    if (!new File("/tmp", "lotr.csv").exists()) {
      logger.info("Copied data from github to data directory.")
      s"curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv " !
    }

  val consumer =
    pulsarController
      .createConsumer("pulsarOutputTest", Schema.BYTES, "ConnectedComponents" + deploymentID)

  logger.info("Consumer created.")

  test("Outputting to Pulsar") {
    val queryProgressTracker =
      graph.rangeQuery(
              graphAlgorithm = ConnectedComponents(),
              outputFormat = outputFormat,
              start = 1,
              end = 32674,
              increment = 10000,
              windows = List(500, 1000, 10000)
      )

    queryProgressTracker.waitForJob()

    val firstResult = new String(receiveMessage(consumer).getValue)

    logger.debug(s"Output to Pulsar complete. First result is: '$firstResult'.")

    assert(firstResult.nonEmpty)
  }

  override def setSchema(): Schema[String] =
    Schema.STRING
}
