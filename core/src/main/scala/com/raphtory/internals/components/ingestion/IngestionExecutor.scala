package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.api.input.Source
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.BlockIngestion
import com.raphtory.internals.components.querymanager.NonBlocking
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[raphtory] class IngestionExecutor(
    graphID: String,
    source: Source,
    blocking: Boolean,
    sourceID: Int,
    conf: Config,
    topics: TopicRepository,
    override val scheduler: Scheduler
) extends Component[Any](conf)
        with FlushToFlight {
  override val logger: Logger                               = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError                                   = conf.getBoolean("raphtory.builders.failOnError")
  override val writers: Map[Int, EndPoint[GraphAlteration]] = topics.graphUpdates(graphID).endPoint()
  private val queryManager                                  = topics.blockingIngestion().endPoint
  private val sourceInstance                                = source.buildSource(graphID, sourceID)
  private val spoutReschedulesCount                         = telemetry.spoutReschedules.labels(graphID)
  private val fileLinesSent                                 = telemetry.fileLinesSent.labels(graphID)

  private var index: Long                                     = 0
  private var scheduledRun: Option[() => Future[Unit]]        = None

  sourceInstance.setupStreamIngestion(writers)

  private def rescheduler(): Unit = {
    sourceInstance.executeReschedule()
    executeSpout()
  }: Unit

  override def stop(): Unit = {
    scheduledRun.foreach(cancelable => cancelable())
    close()
    writers.values.foreach(_.close())
  }

  override def run(): Unit = {
    logger.debug("Running ingestion executor")
    executeSpout()
  }

  override def handleMessage(msg: Any): Unit = {} //No messages received by this component

  private def executeSpout(): Unit = {
    spoutReschedulesCount.inc()
    var iBlocked = false

    if (sourceInstance.hasRemainingUpdates)
      if (blocking) {
        queryManager.sendAsync(BlockIngestion(sourceID = sourceInstance.sourceID, graphID = graphID))
        iBlocked = true
      }
      else
        queryManager.sendAsync(NonBlocking(sourceID = sourceInstance.sourceID, graphID = graphID))

    while (sourceInstance.hasRemainingUpdates) {
      latestMsgTimeToFlushToFlight = System.currentTimeMillis()
      fileLinesSent.inc()
      index = index + 1
      sourceInstance.sendUpdates(index, failOnError)
    }
    if (sourceInstance.spoutReschedules())
      reschedule()
    if (blocking && iBlocked)
      queryManager.sendAsync(
              UnblockIngestion(
                      sourceInstance.sourceID,
                      graphID = graphID,
                      sourceInstance.sentMessages(),
                      sourceInstance.highestTimeSeen(),
                      force = false
              )
      )
  }

  private def reschedule(): Unit = {
    // TODO: Parameterise the delay
    logger.trace("Spout: Scheduling spout to poll again in 1 seconds.")
    scheduledRun = Option(scheduler.scheduleOnce(1.seconds, rescheduler()))
  }

}

object IngestionExecutor {

  def apply[IO[_]: Spawn](
      graphID: String,
      source: Source,
      blocking: Boolean,
      sourceID: Int,
      config: Config,
      topics: TopicRepository
  )(implicit IO: Async[IO]): Resource[IO, IngestionExecutor] =
    Component
      .makeAndStart(
              topics,
              "spout-executor",
              Seq.empty[CanonicalTopic[Any]],
              new IngestionExecutor(graphID, source, blocking, sourceID, config, topics, new Scheduler)
      )

}
