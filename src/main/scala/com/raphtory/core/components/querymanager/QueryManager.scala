package com.raphtory.core.components.querymanager

import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.collection.mutable

/** @DoNotDocument */
class QueryManager(scheduler: Scheduler, conf: Config, pulsarController: PulsarController)
        extends Component[Array[Byte]](conf: Config, pulsarController: PulsarController) {
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

  override def handleMessage(msg: Array[Byte]): Unit =
    deserialise[QueryManagement](msg) match {
      case query: Query             =>
        val jobID        = query.name
        logger.debug(
                s"Handling query name: ${query.name}, start: ${query.startTime}, end: ${query.endTime}, increment: ${query.increment}, windows: ${query.windows}"
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
        logger.debug(s"Setting watermark to '$watermark' for partition '${watermark.partitionID}'.")
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
