package com.raphtory.pulsar

import cats.effect.Async
import cats.effect.IO
import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.lotrtest.LOTRGraphBuilder
import com.raphtory.pulsar.connector.PulsarConnector
import com.raphtory.pulsar.sink.PulsarSink
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import java.net.URL
import java.util.UUID
import scala.language.postfixOps

class PulsarOutputTest extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish = false) {
  withGraph.test("Outputting to Pulsar") { graph: TemporalGraph =>
    graph.load(Source(setSpout(), setGraphBuilder()))

    val config = Raphtory.getDefaultConfig()
    PulsarConnector[IO](config).use { pulsarConnector =>
      val salt = UUID.randomUUID().toString

      Async[IO].bracket(
              IO {
                pulsarConnector
                  .createSharedConsumer(
                          subscriptionName = "pulsarOutputTest",
                          schema = Schema.BYTES,
                          topics = "EdgeList" + salt
                  )
              }
      ) { consumer =>
        IO {
          val sink: PulsarSink     = PulsarSink("EdgeList" + salt)
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

  def filePath = s"/tmp/lotr.csv"

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] =
    consumer.receive

  override def setSpout(): Spout[String] = FileSpout(filePath)

  override def setGraphBuilder(): GraphBuilder[String] = LOTRGraphBuilder

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(filePath -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))

}
