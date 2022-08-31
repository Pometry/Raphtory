package com.raphtory.internals.management

import com.raphtory.api.input.Source
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.BlockIngestion
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.DynamicLoader
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.management.id.IDManager
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

private[raphtory] class QuerySender(
    private val scheduler: Scheduler,
    private val topics: TopicRepository,
    private val config: Config,
    private val idManager: IDManager,
    private val clientID: String
) {

  class NoIDException(message: String) extends Exception(message)

  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val graphID           = config.getString("raphtory.graph.id")
  val partitionServers: Int     = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int  = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int      = partitionServers * partitionsPerServer
  private lazy val writers      = topics.graphUpdates(graphID).endPoint
  private lazy val queryManager = topics.blockingIngestion.endPoint
  private lazy val submissions  = topics.submissions.endPoint
  private val blockingSources   = ArrayBuffer[Int]()
  protected var scheduledRunArrow: Option[() => Future[Unit]] = Option(scheduler.scheduleOnce(1.seconds, flushArrow()))

  def getSourceID(): Int =
    idManager.getNextAvailableID() match {
      case Some(id) =>
        id
      case None     =>
        throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID")
    }

  private def flushArrow(): Unit = {
    writers.values.foreach(_.flushAsync())
    runArrow()
  }: Unit

  private def runArrow(): Unit =
    scheduledRunArrow = Option(scheduler.scheduleOnce(1.seconds, flushArrow()))

  def submit(query: Query, customJobName: String = ""): QueryProgressTracker = {
    val jobName     = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID       = jobName + "_" + Random.nextLong().abs
    val outputQuery = query.copy(name = jobID, blockedBy = blockingSources.toArray)

    topics.submissions.endPoint sendAsync outputQuery

    val tracker = QueryProgressTracker.unsafeApply(jobID, config, topics)
    scheduler.execute(tracker)
    tracker
  }

  def blockIngestion(sourceID: Int): Unit = {
    blockingSources += sourceID
    queryManager.sendAsync(BlockIngestion(sourceID, graphID))
  }

  def unblockIngestion(sourceID: Int, index: Long, force: Boolean): Unit =
    queryManager.sendAsync(
            UnblockIngestion(
                    sourceID,
                    graphID = graphID,
                    index,
                    force
            )
    )

  def destroyGraph(force: Boolean): Unit =
    topics.graphSetup.endPoint sendAsync DestroyGraph(graphID, clientID, force)

  def disconnect(): Unit =
    topics.graphSetup.endPoint sendAsync ClientDisconnected(graphID, clientID)

  def establishGraph(): Unit =
    topics.graphSetup.endPoint sendAsync EstablishGraph(graphID, clientID)

  def individualUpdate(update: GraphUpdate) =
    writers((update.srcId % totalPartitions).toInt) sendAsync update

  def submitSource(blocking: Boolean, sources: Seq[Source], id: String): Unit = {

    val clazzes      = sources
      .map { source =>
        source.getBuilder.getClass
      }
      .toSet
      .asInstanceOf[Set[Class[_]]]
    val sourceWithId = sources.map { source =>
      idManager.getNextAvailableID() match {
        case Some(id) =>
          if (blocking) blockingSources += id
          (id, source)
        case None     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID for $source")
      }
    }
    submissions sendAsync IngestData(DynamicLoader(clazzes), id, sourceWithId, blocking)
  }

  def closeArrow() =
    writers

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
