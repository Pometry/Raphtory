package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
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
      val producer       = pulsarController.createProducer(Schema.BYTES, producer_topic)

      logger.debug(
              s"Producer for '$fileDataPath' created '$producer' with topic '$producer_topic'."
      )

      var lineNo = 1
      var array  = ArrayBuffer[String]()
      for (line <- source.getLines()) {
        array += s"$line $lineNo"
        lineNo += 1
        if (lineNo % 100000 == 0) {
          sendBatch(producer, array.toArray)
          array = ArrayBuffer[String]()
          logger.info(s"Static Graph Spout sent $lineNo messages")
        }
      }
      sendBatch(producer, array.toArray)

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
