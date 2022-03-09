package com.raphtory.core.components.spout

import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Message

import java.util.concurrent.TimeUnit
import scala.reflect.runtime.universe.TypeTag

class SpoutExecutor[T](
    spout: Spout[T],
    conf: Config,
    private val pulsarController: PulsarController,
    scheduler: Scheduler
) extends Component[T](conf: Config, pulsarController: PulsarController) {
  protected val failOnError: Boolean = conf.getBoolean("raphtory.spout.failOnError")
  private var linesProcessed: Int    = 0

  private val producer = pulsarController.toBuildersProducer()

  override def stop(): Unit = producer.close()

  override def handleMessage(msg: T): Unit = {} //Currently nothing to listen to here

  override def run(): Unit =
    executeSpout()

  private def executeSpout() = {
    while (spout.hasNext()) {
      linesProcessed = linesProcessed + 1
      if (linesProcessed % 1000 == 0)
        logger.debug(s"Spout: sent $linesProcessed messages.")
      spout.next() match {
        case Some(data) => producer.sendAsync(serialise(data))
        case None       =>
      }
    }
    if (spout.reschedule())
      reschedule()
  }

  private def reschedule(): Unit = {
    val runnable = new Runnable {
      override def run(): Unit = executeSpout()
    }
    // TODO: Parameterise the delay
    logger.debug("Spout: Scheduling spout to poll again in 10 seconds.")
    scheduler.scheduleOnce(10, TimeUnit.SECONDS, runnable)
  }

}
