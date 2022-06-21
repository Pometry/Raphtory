package com.raphtory.generic

import cats.effect.Async
import cats.effect.IO
import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema
import org.scalatest.Ignore

import java.io.File
import java.net.URL
import scala.language.postfixOps
import scala.sys.process._

@Ignore
class PulsarOutputTest extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish = false) {
  withGraph.test("Outputting to Pulsar") { graph: DeployedTemporalGraph =>
    PulsarConnector[IO](graph.config).use { pulsarConnector =>
      val deploymentId = graph.deploymentId

      Async[IO].bracket(
              IO {
                pulsarConnector
                  .createSharedConsumer(
                          subscriptionName = "pulsarOutputTest",
                          schema = Schema.BYTES,
                          topics = "EdgeList" + deploymentId
                  )
              }
      ) { consumer =>
        IO {
          val sink: PulsarSink     = PulsarSink("EdgeList" + deploymentId)
          val queryProgressTracker =
            graph
              .range(1, 32674, 10000)
              .window(List(500, 1000, 10000), Alignment.END)
              .execute(EdgeList())
              .writeTo(sink, "EdgeList")
          jobId = queryProgressTracker.getJobId
          queryProgressTracker.waitForJob()
          val firstResult          = new String(receiveMessage(consumer).getValue)
          logger.info(s"Output to Pulsar complete. First result is: '$firstResult'.")

          assert(firstResult.nonEmpty)
        }
      }(c => IO(c.unsubscribe()))

    }
  }

  override def batchLoading(): Boolean = false

  def filePath = s"/tmp/lotr.csv"

  override def setSpout(): Spout[String] = FileSpout(filePath)

  override def setGraphBuilder(): GraphBuilder[String] = new LOTRGraphBuilder()

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(filePath -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))

}
