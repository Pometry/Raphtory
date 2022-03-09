package com.raphtory.generic

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.components.spout.SpoutExecutor
import com.raphtory.config.PulsarController
import com.raphtory.deploy.Raphtory
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

class PulsarOutputTest extends BaseRaphtoryAlgoTest[String] {
  val outputFormat: PulsarOutputFormat = PulsarOutputFormat("EdgeList" + deploymentID)
  override def batchLoading(): Boolean = false

  override def setSpout(): Spout[String]               = FileSpout(s"/tmp/lotr.csv")
  override def setGraphBuilder(): GraphBuilder[String] = new LOTRGraphBuilder()

  override def setup() =
    if (!new File("/tmp", "lotr.csv").exists()) {
      logger.info("Copied data from github to data directory.")
      s"curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv " !
    }

  val consumer =
    pulsarController
      .createSharedConsumer("pulsarOutputTest", Schema.BYTES, "EdgeList" + deploymentID)

  logger.info("Consumer created.")

  test("Outputting to Pulsar") {
    val queryProgressTracker =
      graph.rangeQuery(
              graphAlgorithm = EdgeList(),
              outputFormat = outputFormat,
              start = 1,
              end = 32674,
              increment = 10000,
              windows = List(500, 1000, 10000)
      )

    queryProgressTracker.waitForJob()

    val firstResult = new String(receiveMessage(consumer).getValue)

    logger.info(s"Output to Pulsar complete. First result is: '$firstResult'.")

    assert(firstResult.nonEmpty)
  }
}
