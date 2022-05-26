package com.raphtory.components.spout

import com.raphtory.communication.TopicRepository
import com.raphtory.components.Component
import com.raphtory.config.MonixScheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Cancelable
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

/** @note DoNotDocument */
class SpoutExecutor[T](
    spout: Spout[T],
    conf: Config,
    topics: TopicRepository,
    scheduler: MonixScheduler
) extends Component[T](conf) {

  protected val failOnError: Boolean           = conf.getBoolean("raphtory.spout.failOnError")
  private var linesProcessed: Int              = 0
  private var scheduledRun: Option[Cancelable] = None
  private val logger: Logger                   = Logger(LoggerFactory.getLogger(this.getClass))

  private val spoutReschedulesCount = telemetry.spoutReschedules.labels(deploymentID)
  private val fileLinesSent         = telemetry.fileLinesSent.labels(deploymentID)

  private val rescheduler: () => Unit = () =>
    {
      spout.executeReschedule()
      executeSpout()
    }: Unit
  private val builders                = topics.spout[T].endPoint

  override def stop(): Unit = {
    scheduledRun.foreach(_.cancel())
    builders.close()
  }

  override def run(): Unit =
    executeSpout()

  override def handleMessage(msg: T): Unit = {} //No messages received by this component

  private def executeSpout() = {
    spoutReschedulesCount.inc()
    while (spout.hasNext) {
      fileLinesSent.inc()
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
    scheduledRun = scheduler.scheduleOnce(10.seconds, rescheduler)
  }

}
