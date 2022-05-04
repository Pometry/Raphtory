package com.raphtory.components.querymanager

import com.raphtory.components.Component
import com.raphtory.config.Scheduler
import com.raphtory.config.TopicRepository
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.collection.mutable

/** @DoNotDocument */
class QueryManager(scheduler: Scheduler, conf: Config, topics: TopicRepository)
        extends Component[QueryManagement](conf) {
  private val currentQueries = mutable.Map[String, QueryHandler]()
  //private val watermarkGlobal                           = pulsarController.globalwatermarkPublisher() TODO: remove?
  private val watermarks     = mutable.Map[Int, WatermarkTime]()

  private val listener = topics
    .registerListener(
            "query-manager",
            handleMessage,
            Seq(topics.queries, topics.watermark, topics.endedQueries)
    )

  override def run(): Unit = {
    logger.debug("Starting Query Manager Consumer.")
    listener.start()
  }

  override def stop(): Unit = {
    listener.close()
    currentQueries.foreach(_._2.stop())
    // watermarkGlobal.close() TODO: remove?
  }

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
            currentQueries.remove(req.jobID)
          case None               => //sender ! QueryNotPresent(req.jobID)
        }
      case watermark: WatermarkTime =>
        logger.debug(
                s"Setting watermark to earliest time '${watermark.startTime}'" +
                  s" and latest time '${watermark.endTime}'" +
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
          minTime = Math.min(minTime, watermark.endTime)
          maxTime = Math.max(maxTime, watermark.endTime)
      }
      if (safe) maxTime else minTime
    }
    else 0 // not received a message from each partition yet
    // watermarkGlobal sendAsync watermark TODO: remove I guess?
    watermark
  }

  def earliestTime(): Option[Long] =
    if (watermarks.size == totalPartitions) {
      val startTimes = watermarks map {
        case (_, watermark) => watermark.startTime
      }
      Some(startTimes.min)
    }
    else
      None
  // not received a message from each partition yet
}
