package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[raphtory] class IngestionExecutor(
    graphID: String,
    source: Source,
    conf: Config,
    topics: TopicRepository,
    scheduler: Scheduler
) extends Component[Any](conf) {
  private val logger: Logger        = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError           = conf.getBoolean("raphtory.builders.failOnError")
  private val writers               = topics.graphUpdates(graphID).endPoint()
  private val sourceInstance        = source.buildSource(graphID)
  private val spoutReschedulesCount = telemetry.spoutReschedules.labels(graphID)
  private val fileLinesSent         = telemetry.fileLinesSent.labels(graphID)

  private var index: Int                                      = 0
  private var scheduledRun: Option[() => Future[Unit]]        = None
  protected var scheduledRunArrow: Option[() => Future[Unit]] = None

  sourceInstance.setupStreamIngestion(writers)

  private def flushArrow(): Unit = {
    writers.values.foreach(_.flushAsync())
    runArrow()
  }: Unit

  private def runArrow(): Unit =
    scheduledRunArrow = Option(scheduler.scheduleOnce(1.seconds, flushArrow()))

  private def runsource(): Unit = {
    sourceInstance.executeReschedule()
    executeSpout()
  }: Unit

  override def stop(): Unit = {
    scheduledRun.foreach(cancelable => cancelable())
    writers.values.foreach(_.close())
  }

  override def run(): Unit = {
    logger.debug("Running ingestion executor")
    runArrow()
    executeSpout()
  }

  override def handleMessage(msg: Any): Unit = {} //No messages received by this component

  private def executeSpout(): Unit = {
    spoutReschedulesCount.inc()
    while (sourceInstance.hasRemainingUpdates) {
      fileLinesSent.inc()
      index = index + 1
      if (index % 100_000 == 0)
        logger.debug(s"Source: sent $index messages.")
      sourceInstance.sendUpdates(index, failOnError)
    }
    if (sourceInstance.spoutReschedules())
      reschedule()
  }

  private def reschedule(): Unit = {
    // TODO: Parameterise the delay
    logger.trace("Spout: Scheduling spout to poll again in 1 seconds.")
    scheduledRun = Option(scheduler.scheduleOnce(1.seconds, runsource()))
  }

}

object IngestionExecutor {

  def apply[IO[_]: Spawn](
      graphID: String,
      source: Source,
      config: Config,
      topics: TopicRepository
  )(implicit IO: Async[IO]): Resource[IO, IngestionExecutor] =
    Component
      .makeAndStart(
              topics,
              "spout-executor",
              Seq.empty[CanonicalTopic[Any]],
              new IngestionExecutor(graphID, source, config, topics, new Scheduler)
      )

}
