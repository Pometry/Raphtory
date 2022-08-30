package com.raphtory.internals.components.querymanager

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.graph.SourceTracker
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.concurrent._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[raphtory] class QueryManager(
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val currentQueries                   = mutable.Map[String, QueryHandler]()
  private val ingestion                        = topics.ingestSetup.endPoint
  val sources: mutable.Map[Int, SourceTracker] = mutable.Map[Int, SourceTracker]()
  var blockedQueries: ArrayBuffer[Query]       = ArrayBuffer[Query]()

  def startBlockIngesting(ID: Int): Unit = {
    logger.info(s"Source '$ID' is blocking analysis for Graph '$graphID'")
    sources.get(ID) match {
      case Some(tracker) => //already created
      case None          => sources.put(ID, SourceTracker())
    }
  }

  def stopBlockIngesting(ID: Int, force: Boolean, msgCount: Long): Unit =
    if (force) {
      logger.info(s"Source '$ID' is forced unblocking analysis for Graph '$graphID' with $msgCount messages sent.")
      sources.foreach {
        case (id, tracker) =>
          tracker.unblock()
          if (id == ID)
            tracker.setTotalSent(msgCount)
          else
            tracker.setTotalSent(0)
      }
    }
    else {
      logger.info(s"Source '$ID' is unblocking analysis for Graph '$graphID' with $msgCount messages sent.")
      sources.get(ID) match {
        case Some(tracker) =>
          tracker.unblock()
          tracker.setTotalSent(msgCount)
        case None          =>
          val tracker = SourceTracker()
          tracker.unblock()
          tracker.setTotalSent(msgCount)
          sources.put(ID, tracker)

      }
    }

  def currentlyBlockIngesting(blockedBy: Array[Int]): Boolean = {
    val generalBlocking = sources
      .map({
        case (id, tracker) => tracker.isBlocking
      })
      .exists(_ == true)

    if (!generalBlocking && blockedBy.nonEmpty)
      !blockedBy.forall(sources.contains)
    else
      generalBlocking
  }

  //private val watermarkGlobal                           = pulsarController.globalwatermarkPublisher() TODO: turn back on when needed
  private val watermarks = TrieMap[Int, WatermarkTime]()

  override def run(): Unit = logger.debug("Starting Query Manager Consumer.")

  override def stop(): Unit =
    currentQueries.synchronized(currentQueries.foreach(_._2.stop()))
  // watermarkGlobal.close() TODO: turn back on when needed

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case ingestData: IngestData       =>
        ingestion sendAsync ingestData

      case blocking: BlockIngestion     =>
        startBlockIngesting(blocking.sourceID)

      case unblocking: UnblockIngestion =>
        stopBlockIngesting(unblocking.sourceID, unblocking.force, unblocking.messageCount)
        checkBlockedQueries()

      case query: Query                 =>
        val jobID = query.name
        logger.debug(s"Handling query: $query")

        query.blockedBy.foreach(source =>
          sources.get(source) match {
            case Some(tracker) => //already created
            case None          => sources.put(source, SourceTracker())
          }
        )

        if (currentlyBlockIngesting(query.blockedBy)) {
          logger.info(s"Query '${query.name}' currently blocked, waiting for ingestion to complete.")
          blockedQueries += query
        }
        else {
          val queryHandler = spawnQuery(jobID, query)
          trackNewQuery(jobID, queryHandler)
        }

      case req: EndQuery                =>
        currentQueries.synchronized {
          currentQueries.get(req.jobID) match {
            case Some(queryhandler) =>
              queryhandler.stop()
              currentQueries.remove(req.jobID)
            case None               => //sender ! QueryNotPresent(req.jobID)
          }
        }
        blockedQueries.filter(q => q.name equals req.jobID)

      case watermark: WatermarkTime     =>
        watermarks.put(watermark.partitionID, watermark)
        watermark.sourceMessages.foreach {
          case (id, count) =>
            sources.get(id) match {
              case Some(tracker) =>
                tracker.setReceivedMessage(watermark.partitionID, count)

              case None          =>
                val tracker = SourceTracker()
                tracker.setReceivedMessage(watermark.partitionID, count)
                sources.put(watermark.partitionID, tracker)
            }
        }
        checkBlockedQueries()

    }

  private def spawnQuery(id: String, query: Query): QueryHandler = {
    logger.info(s"Query '${query.name}' received, your job ID is '$id'.")

    val queryHandler = new QueryHandler(this, scheduler, id, query, conf, topics, query.pyScript)
    scheduler.execute(queryHandler)
    telemetry.totalQueriesSpawned.labels(graphID).inc()
    queryHandler
  }

  private def checkBlockedQueries(): Unit =
    blockedQueries = blockedQueries.filter { query =>
      if (!currentlyBlockIngesting(query.blockedBy)) {
        val queryHandler = spawnQuery(query.name, query)
        trackNewQuery(query.name, queryHandler)
        false
      }
      else true
    }

  private def trackNewQuery(jobID: String, queryHandler: QueryHandler): Unit =
    //sender() ! ManagingTask(queryHandler)
    currentQueries.synchronized(currentQueries += ((jobID, queryHandler)))

  def latestTime(graphID: String): Long = {
    val watermark = if (watermarks.size == totalPartitions) {
      var safe    = true
      var minTime = Long.MaxValue
      var maxTime = Long.MinValue
      watermarks
        .foreach {
          case (partition, watermark) =>
            safe = watermark.safe && safe
            minTime = Math.min(minTime, watermark.latestTime)
            maxTime = Math.max(maxTime, watermark.latestTime)
            telemetry.globalWatermarkMin.labels(graphID).set(minTime.toDouble)
            telemetry.globalWatermarkMax.labels(graphID).set(maxTime.toDouble)
        }
      if (safe) maxTime else minTime
    }
    else 0 // not received a message from each partition yet
    // watermarkGlobal sendAsync watermark TODO: turn back on when needed
    watermark
  }

  def earliestTime(): Option[Long] =
    if (!currentlyBlockIngesting(Array()) && watermarks.size == totalPartitions) {
      val startTimes = watermarks.map { case (_, watermark) => watermark.oldestTime }
      Some(startTimes.min)
    }
    else
      None
}

object QueryManager {

  import cats.effect.Spawn

  def apply[IO[_]: Async: Spawn](
      config: Config,
      topics: TopicRepository
  ): Resource[IO, QueryManager] = {
    val scheduler = new Scheduler
    val topicList = List(topics.submissions, topics.watermark, topics.completedQueries, topics.blockingIngestion)
    Component.makeAndStart[IO, QueryManagement, QueryManager](
            topics,
            "query-manager",
            topicList,
            new QueryManager(scheduler, config, topics)
    )
  }

}
