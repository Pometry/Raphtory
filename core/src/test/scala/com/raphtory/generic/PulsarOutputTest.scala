package com.raphtory.generic

import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Schema

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

class PulsarOutputTest extends BaseRaphtoryAlgoTest[String] {
  test("Outputting to Pulsar") {
    val outputFormat: PulsarOutputFormat = PulsarOutputFormat("EdgeList" + deploymentID)

    val consumer: Consumer[Array[Byte]] =
      pulsarController
        .createSharedConsumer(
                subscriptionName = "pulsarOutputTest",
                schema = Schema.BYTES,
                topics = "EdgeList" + deploymentID
        )

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

  override def batchLoading(): Boolean = false

  override def setSpout(): Spout[String] = FileSpout(s"/tmp/lotr.csv")

  override def setGraphBuilder(): GraphBuilder[String] = new LOTRGraphBuilder()

  override def setup(): Unit = {
    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

    if (!new File(path).exists())
      try s"curl -o $path $url" !!
      catch {
        case ex: Exception =>
          logger.error(s"Failed to download 'lotr.csv' due to ${ex.getMessage}.")
          ex.printStackTrace()

          (s"rm $path" !)
          throw ex
      }
  }

}
