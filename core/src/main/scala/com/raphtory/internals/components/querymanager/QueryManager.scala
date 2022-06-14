package com.raphtory.internals.components.querymanager

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.concurrent._
import scala.collection.mutable

private[raphtory] class QueryManager(
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val currentQueries = mutable.Map[String, QueryHandler]()

  //private val watermarkGlobal                           = pulsarController.globalwatermarkPublisher() TODO: turn back on when needed
  private val watermarks: Map[Int, WatermarkTime] = new TrieMap[Int, WatermarkTime]()

  override def run(): Unit =
    logger.debug("Starting Query Manager Consumer.")

  override def stop(): Unit =
    currentQueries.foreach(_._2.stop())
  // watermarkGlobal.close() TODO: turn back on when needed

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case query: Query             =>
        val jobID        = query.name
        logger.debug(
                s"Handling query: $query"
        )
        val queryHandler = spawnQuery(jobID, query)
        trackNewQuery(jobID, queryHandler)

      case req: EndQuery            =>
        currentQueries.get(req.jobID) match {
          case Some(queryhandler) =>
            queryhandler.stop()
            currentQueries.remove(req.jobID)
          case None               => //sender ! QueryNotPresent(req.jobID)
        }
      case watermark: WatermarkTime =>
        logger.debug(
                s"Setting watermark to earliest time '${watermark.oldestTime}'" +
                  s" and latest time '${watermark.latestTime}'" +
                  s" for partition '${watermark.partitionID}'."
        )
        watermarks.put(watermark.partitionID, watermark)
    }

  private def spawnQuery(id: String, query: Query): QueryHandler = {
    logger.info(s"Query '${query.name}' received, your job ID is '$id'.")

    val queryHandler = new QueryHandler(
            this,
            scheduler,
            id,
            query,
            conf,
            topics
    )
    scheduler.execute(queryHandler)
    telemetry.totalQueriesSpawned.labels(deploymentID).inc()
    queryHandler
  }

  private def trackNewQuery(jobID: String, queryHandler: QueryHandler): Unit =
    //sender() ! ManagingTask(queryHandler)
    currentQueries += ((jobID, queryHandler))

  def latestTime(): Long = {
    val watermark = if (watermarks.size == totalPartitions) {
      var safe    = true
      var minTime = Long.MaxValue
      var maxTime = Long.MinValue

      watermarks.foreach {
        case (key, watermark) =>
          safe = watermark.safe && safe
          minTime = Math.min(minTime, watermark.latestTime)
          maxTime = Math.max(maxTime, watermark.latestTime)
          telemetry.globalWatermarkMin.labels(deploymentID).set(minTime.toDouble)
          telemetry.globalWatermarkMax.labels(deploymentID).set(maxTime.toDouble)
      }
      if (safe) maxTime else minTime
    }
    else 0 // not received a message from each partition yet
    // watermarkGlobal sendAsync watermark TODO: turn back on when needed
    watermark
  }

  def earliestTime(): Option[Long] =
    if (watermarks.size == totalPartitions) {
      val startTimes = watermarks map {
        case (_, watermark) => watermark.oldestTime
      }
      Some(startTimes.min)
    }
    else
      None
  // not received a message from each partition yet
}

object QueryManager {

  import cats.effect.Spawn

  def apply[IO[_]: Async: Spawn](
      config: Config,
      topics: TopicRepository
  ): Resource[IO, QueryManager] = {
    val scheduler = new Scheduler
    Component.makeAndStart(
            topics,
            "query-manager",
            Seq(topics.submissions, topics.watermark, topics.completedQueries),
            new QueryManager(scheduler, config, topics)
    )
  }

}
