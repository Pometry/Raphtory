package com.raphtory.components.spout

import com.raphtory.communication.TopicRepository
import com.raphtory.components.Component
import com.raphtory.config.Cancelable
import com.raphtory.config.Scheduler
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message

import java.util.concurrent.TimeUnit
import scala.reflect.runtime.universe.TypeTag
import com.raphtory.config.telemetry.ComponentTelemetryHandler
import com.raphtory.config.telemetry.SpoutTelemetry


import scala.concurrent.duration.DurationInt

/** @note DoNotDocument */
class SpoutExecutor[T](
    spout: Spout[T],
    conf: Config,
    topics: TopicRepository,
    scheduler: Scheduler
) extends Component[T](conf) {
  protected val failOnError: Boolean           = conf.getBoolean("raphtory.spout.failOnError")
  private var linesProcessed: Int              = 0
  private var scheduledRun: Option[Cancelable] = None

  val rescheduler: () => Unit = () =>
    {
      spout.executeReschedule()
      executeSpout()
    }: Unit
  private val builders        = topics.spout[T].endPoint

  override def stop(): Unit = {
    scheduledRun.foreach(_.cancel())
    builders.close()
  }

  override def run(): Unit =
    executeSpout()

  override def handleMessage(msg: T): Unit = {} //No messages received by this component

  private def executeSpout() = {
    telemetry.spoutReschedules.labels(deploymentID).inc()
    while (spout.hasNext) {
      telemetry.fileLinesSent.labels(deploymentID).inc()
      linesProcessed = linesProcessed + 1
      if (linesProcessed % 100_000 == 0)
        logger.debug(s"Spout: sent $linesProcessed messages.")
      builders sendAsync spout.next()
    }
    if (spout.spoutReschedules())
      reschedule()
  }

  private def reschedule(): Unit = {
    // TODO: Parameterise the delay
    logger.debug("Spout: Scheduling spout to poll again in 10 seconds.")
    scheduledRun = Some(scheduler.scheduleOnce(10.seconds, rescheduler))
  }

}
