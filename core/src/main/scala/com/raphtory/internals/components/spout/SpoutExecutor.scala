package com.raphtory.internals.components.spout

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.Topic
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[raphtory] class SpoutExecutor[T](
    spout: Spout[T],
    conf: Config,
    topics: TopicRepository,
    scheduler: Scheduler
) extends Component[T](conf) {

  protected val failOnError: Boolean                   = conf.getBoolean("raphtory.spout.failOnError")
  private var linesProcessed: Int                      = 0
  private var scheduledRun: Option[() => Future[Unit]] = None
  private val logger: Logger                           = Logger(LoggerFactory.getLogger(this.getClass))

  private val spoutReschedulesCount = telemetry.spoutReschedules.labels(deploymentID)
  private val fileLinesSent         = telemetry.fileLinesSent.labels(deploymentID)

  private def rescheduler(): Unit = {
    spout.executeReschedule()
    executeSpout()
  }: Unit

  private val builders: EndPoint[T] = topics.spout[T].endPoint

  override def stop(): Unit = {
    scheduledRun.foreach(cancelable => cancelable())
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
    scheduledRun = Option(scheduler.scheduleOnce(1.seconds, rescheduler()))
  }

}

object SpoutExecutor {

  def apply[IO[_]: Spawn, SP](
      spout: Spout[SP],
      config: Config,
      topics: TopicRepository
  )(implicit IO: Async[IO]): Resource[IO, SpoutExecutor[SP]] =
    Component
      .makeAndStart(
              topics,
              "spout-executor",
              Seq.empty[CanonicalTopic[SP]],
              new SpoutExecutor[SP](spout, config, topics, new Scheduler)
      )

}
