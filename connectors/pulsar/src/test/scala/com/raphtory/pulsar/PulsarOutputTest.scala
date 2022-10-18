package com.raphtory.pulsar

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import com.raphtory.BaseRaphtoryAlgoTest
import com.raphtory.TestUtils
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.api.input.Source
import com.raphtory.internals.context.RaphtoryContext.RaphtoryContextBuilder
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.pulsar.connector.PulsarConnector
import com.raphtory.pulsar.sink.PulsarSink
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema
import java.net.URL
import java.util.UUID
import scala.language.postfixOps

class PulsarOutputTest extends BaseRaphtoryAlgoTest[String](deleteResultAfterFinish = false) {

  test("Outputting to Pulsar".ignore) {
    val out = for {
      _          <- TestUtils.manageTestFile(liftFileIfNotPresent)
      ctxBuilder <- Resource.fromAutoCloseable(IO.delay(RaphtoryContextBuilder()))
    } yield ctxBuilder.local()

    out
      .use { ctx =>
        val config = ConfigBuilder().build().getConfig
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
                    .window(List(500, 1000, 10000), Alignment.END)
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
      .unsafeRunSync()
  }

  def filePath = s"/tmp/lotr.csv"

  def receiveMessage(consumer: Consumer[Array[Byte]]): Message[Array[Byte]] = consumer.receive

  override def setSource(): Source = CSVEdgeListSource.fromFile(filePath)

  override def liftFileIfNotPresent: Option[(String, URL)] =
    Some(filePath -> new URL("https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"))
}
