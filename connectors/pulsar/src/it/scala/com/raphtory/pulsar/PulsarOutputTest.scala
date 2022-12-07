package com.raphtory.pulsar

import cats.effect.Async
import cats.effect.IO
import com.raphtory._
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.pulsar.connector.PulsarConnector
import com.raphtory.pulsar.sink.PulsarSink
import com.raphtory.sources.CSVEdgeListSource
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import java.net.URL
import java.util.UUID
import scala.language.postfixOps

class PulsarOutputTest extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish = false) {

  test("Outputting to Pulsar".ignore) {
    val ctx    = new RaphtoryContext(RaphtoryServiceBuilder.standalone[IO](defaultConf), defaultConf)
    val config = ConfigBuilder.getDefaultConfig
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
          ctx.runWithNewGraph() { graph =>
            val sink: PulsarSink     = PulsarSink("EdgeList" + salt)
            val queryProgressTracker =
              graph
                .range(1, 32674, 10000)
                .window(Array(500L, 1000L, 10000L), Alignment.END)
                .execute(EdgeList())
                .writeTo(sink, "EdgeList")
            jobId = queryProgressTracker.getJobId
            queryProgressTracker.waitForJob()
            val firstResult          = new String(receiveMessage(consumer).getValue)
            logger.info(s"Output to Pulsar complete. First result is: '$firstResult'.")

            assert(firstResult.nonEmpty)
          }
        }
      }(c => IO(c.unsubscribe()))
    }
  }

  def filePath = s"/tmp/lotr.csv"

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] = consumer.receive

  override def setSource(): Source = CSVEdgeListSource.fromFile(filePath)

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(filePath -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))
}
