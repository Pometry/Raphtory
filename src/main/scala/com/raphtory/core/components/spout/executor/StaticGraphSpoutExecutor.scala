package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

import scala.io.Source

class StaticGraphSpoutExecutor(
    fileDataPath: String,
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends SpoutExecutor[String](
                conf: Config,
                pulsarController: PulsarController,
                scheduler: Scheduler
        ) {

  private def readFile(fileDataPath: String): Unit =
    try {
      // We assume that Pulsar standalone is running on the users machine before continuing
      // setup and create a producer
      val producer_topic = conf.getString("raphtory.spout.topic")
      val source         = Source.fromFile(fileDataPath)
      val producer       = toBuildersProducer(Schema.STRING)

      logger.debug(
              s"Producer for '$fileDataPath' created '$producer' with topic '$producer_topic'."
      )

      var lineNo = 1
      for (line <- source.getLines()) {
        producer.sendAsync(s"$line $lineNo")
        lineNo += 1
      }

      logger.debug(s"Spout for '$fileDataPath' finished, edge count: ${lineNo - 1}")

      // shutdown
      source.close()
    }
    catch {
      case _: java.util.concurrent.TimeoutException =>
        logger.error("Timed out waiting to read a file.")
        // TODO Better error handling / recovery...
        assert(false)
    }

  override def run(): Unit = {
    logger.info(s"Reading data from '$fileDataPath'.")

    readFile(fileDataPath)
  }

  override def stop(): Unit =
    logger.debug(s"Stopping spout for '$fileDataPath'.")

}
