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
    deploymentID: String,
    source: Source,
    conf: Config,
    topics: TopicRepository,
    scheduler: Scheduler
) extends Component[Any](conf) {
  private val logger: Logger        = Logger(LoggerFactory.getLogger(this.getClass))
  private val writers               = topics.graphUpdates.endPoint
  private val sourceExecutor        = source.buildSource(deploymentID)
  private val spoutReschedulesCount = telemetry.spoutReschedules.labels(deploymentID)
  private val fileLinesSent         = telemetry.fileLinesSent.labels(deploymentID)

  private var linesProcessed: Int                      = 0
  private var scheduledRun: Option[() => Future[Unit]] = None

  private def rescheduler(): Unit = {
    sourceExecutor.executeReschedule()
    executeSpout()
  }: Unit

  override def stop(): Unit = {
    scheduledRun.foreach(cancelable => cancelable())
    writers.values.foreach(_.close())
  }

  override def run(): Unit = {
    logger.info("running ingestion executor")
    executeSpout()
  }

  override def handleMessage(msg: Any): Unit = {} //No messages received by this component

  private def executeSpout(): Unit = {
    spoutReschedulesCount.inc()
    while (sourceExecutor.hasNext) {
      fileLinesSent.inc()
      linesProcessed = linesProcessed + 1
      if (linesProcessed % 100_000 == 0)
        logger.debug(s"Spout: sent $linesProcessed messages.")
      val update = sourceExecutor.next()
      writers(getWriter(update.srcId)).sendAsync(update)
    }
    if (sourceExecutor.spoutReschedules())
      reschedule()
  }

  private def reschedule(): Unit = {
    // TODO: Parameterise the delay
    logger.debug("Spout: Scheduling spout to poll again in 10 seconds.")
    scheduledRun = Option(scheduler.scheduleOnce(1.seconds, rescheduler()))
  }

}

object IngestionExecutor {

  def apply[IO[_]: Spawn](
      deploymentID: String,
      source: Source,
      config: Config,
      topics: TopicRepository
  )(implicit IO: Async[IO]): Resource[IO, IngestionExecutor] =
    Component
      .makeAndStart(
              topics,
              "spout-executor",
              Seq.empty[CanonicalTopic[Any]],
              new IngestionExecutor(deploymentID, source, config, topics, new Scheduler)
      )

}
