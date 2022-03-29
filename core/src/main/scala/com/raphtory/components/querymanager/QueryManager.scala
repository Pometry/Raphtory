package com.raphtory.components.querymanager

import com.raphtory.components.querymanager.handler.RangeQueryHandler
import com.raphtory.components.Component
import com.raphtory.components.querymanager.handler.LiveQueryHandler
import com.raphtory.components.querymanager.handler.PointQueryHandler
import com.raphtory.components.querymanager.handler.RangeQueryHandler
import com.raphtory.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.collection.mutable

/** @DoNotDocument */
class QueryManager(scheduler: Scheduler, conf: Config, pulsarController: PulsarController)
        extends Component[QueryManagement](conf: Config, pulsarController: PulsarController) {
  private val currentQueries                            = mutable.Map[String, QueryHandler]()
  private val watermarkGlobal                           = pulsarController.globalwatermarkPublisher()
  private val watermarks                                = mutable.Map[Int, WatermarkTime]()
  var cancelableConsumer: Option[Consumer[Array[Byte]]] = None

  override def run(): Unit = {
    logger.debug("Starting Query Manager Consumer.")

    cancelableConsumer = Some(pulsarController.startQueryManagerConsumer(messageListener()))
  }

  override def stop(): Unit = {
    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }
    currentQueries.foreach(_._2.stop())
    watermarkGlobal.close()
  }

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case query: PointQuery        =>
        val jobID        = query.name
        logger.debug(
                s"Handling query name: ${query.name}, windows: ${query.windows}, timestamp: ${query.timestamp}, algorithm: ${query.algorithm}"
        )
        val queryHandler = spawnPointQuery(jobID, query)
        trackNewQuery(jobID, queryHandler)

      case query: RangeQuery        =>
        val jobID        = query.name
        logger.debug(
                s"Handling query name: ${query.name}, windows: ${query.windows}, algorithm: ${query.algorithm}"
        )
        val queryHandler = spawnRangeQuery(jobID, query)
        trackNewQuery(jobID, queryHandler)

      case query: LiveQuery         =>
        val jobID        = query.name
        logger.debug(
                s"Handling query name: ${query.name}, windows: ${query.windows}, algorithm: ${query.algorithm}"
        )
        val queryHandler = spawnLiveQuery(jobID, query)
        trackNewQuery(jobID, queryHandler)

      case req: EndQuery            =>
        currentQueries.get(req.jobID) match {
          case Some(queryhandler) =>
            currentQueries.remove(req.jobID)
          case None               => //sender ! QueryNotPresent(req.jobID)
        }
      case watermark: WatermarkTime =>
        logger.debug(s"Setting watermark to '$watermark' for partition '${watermark.partitionID}'.")
        watermarks.put(watermark.partitionID, watermark)
    }

  private def spawnPointQuery(id: String, query: PointQuery): QueryHandler = {
    logger.info(s"Point Query '${query.name}' received, your job ID is '$id'.")

    val queryHandler = new PointQueryHandler(
            this,
            scheduler,
            id,
            query.algorithm,
            query.timestamp,
            query.windows,
            query.outputFormat,
            conf,
            pulsarController
    )
    scheduler.execute(queryHandler)
    queryHandler
  }

  private def spawnRangeQuery(id: String, query: RangeQuery): QueryHandler = {
    logger.info(s"Range Query '${query.name}' received, your job ID is '$id'.")

    val queryHandler = new RangeQueryHandler(
            this,
            scheduler,
            id,
            query.algorithm,
            query.start,
            query.end,
            query.increment,
            query.windows,
            query.outputFormat,
            conf: Config,
            pulsarController: PulsarController
    )
    scheduler.execute(queryHandler)
    queryHandler
  }

  private def spawnLiveQuery(id: String, query: LiveQuery): QueryHandler = {
    logger.info(s"Live Query '${query.name}' received, your job ID is '$id'.")

    val queryHandler = new LiveQueryHandler(
            this,
            scheduler,
            id,
            query.algorithm,
            query.increment,
            query.windows,
            query.outputFormat,
            conf,
            pulsarController
    )
    scheduler.execute(queryHandler)
    queryHandler
  }

  private def trackNewQuery(jobID: String, queryHandler: QueryHandler): Unit =
    //sender() ! ManagingTask(queryHandler)
    currentQueries += ((jobID, queryHandler))

  def whatsTheTime(): Long = {
    val watermark = if (watermarks.size == totalPartitions) {
      var safe    = true
      var minTime = Long.MaxValue
      var maxTime = Long.MinValue
      watermarks.foreach {
        case (key, watermark) =>
          safe = watermark.safe && safe
          minTime = Math.min(minTime, watermark.time)
          maxTime = Math.max(maxTime, watermark.time)
      }
      if (safe) maxTime else minTime
    }
    else 0 // not received a message from each partition yet
    watermarkGlobal.sendAsync(serialise(watermark))
    watermark
  }
}
