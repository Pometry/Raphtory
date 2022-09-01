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

  private val graphID                = config.getString("raphtory.graph.id")
  val partitionServers: Int          = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int       = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int           = partitionServers * partitionsPerServer
  private lazy val writers           = topics.graphUpdates(graphID).endPoint
  private lazy val queryManager      = topics.blockingIngestion.endPoint
  private lazy val submissions       = topics.submissions.endPoint
  private val blockingSources        = ArrayBuffer[Int]()
  private var totalUpdateIndex       = 0     //used at the secondary index for the client
  private var indexSinceLastIDChange = 0     //used to know how many messages to wait for when blocking in the Q manager
  private var lastQueryIndex         = 0     //used to see if a prior query already sent out the unblocking message
  private var newIDRequiredOnUpdate  = false // has a query been since since the last update and do I need a new ID
  private var currentSourceID        = -1

  def sourceID(update: Boolean): Int = {

    if (
            currentSourceID == -1 || (update && newIDRequiredOnUpdate)
    ) //updates the sourceID if we haven't had one yet or if the user has sent a query since the last update block
      idManager.getNextAvailableID() match {
        case Some(id) =>
          currentSourceID = id
          newIDRequiredOnUpdate = false
        case None     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID")
      }

    currentSourceID
  }

  def getIndex: Int = totalUpdateIndex

  def submit(query: Query, customJobName: String = ""): QueryProgressTracker = {

    if (lastQueryIndex < totalUpdateIndex) {
      val source = sourceID(false)
      unblockIngestion(sourceID = source, indexSinceLastIDChange, force = false)
      lastQueryIndex = totalUpdateIndex
      blockingSources += source
      indexSinceLastIDChange = 0
      newIDRequiredOnUpdate = true
    }

    val jobName     = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID       = jobName + "_" + Random.nextLong().abs
    val outputQuery = query.copy(name = jobID, blockedBy = blockingSources.toArray)

    topics.submissions.endPoint sendAsync outputQuery

    val tracker = QueryProgressTracker.unsafeApply(jobID, config, topics)
    scheduler.execute(tracker)
    tracker
  }

  private def unblockIngestion(sourceID: Int, index: Long, force: Boolean): Unit =
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

  def individualUpdate(update: GraphUpdate) = {
    writers((update.srcId % totalPartitions).toInt) sendAsync update
    totalUpdateIndex += 1
    indexSinceLastIDChange += 1

  }

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

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
