package com.raphtory.internals.management

import com.raphtory.api.input.Source
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.BlockIngestion
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.DynamicLoader
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.graph.GraphAlteration
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
    override val scheduler: Scheduler,
    private val topics: TopicRepository,
    private val config: Config,
    private val idManager: IDManager,
    private val clientID: String
) extends FlushToFlight {

  class NoIDException(message: String) extends Exception(message)

  override val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val graphID                                            = config.getString("raphtory.graph.id")
  val partitionServers: Int                                      = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int                                   = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int                                       = partitionServers * partitionsPerServer
  override lazy val writers: Map[Int, EndPoint[GraphAlteration]] = topics.graphUpdates(graphID).endPoint()
  private lazy val queryManager                                  = topics.blockingIngestion.endPoint
  private lazy val submissions                                   = topics.submissions.endPoint
  private val blockingSources                                    = ArrayBuffer[Long]()
  private var totalUpdateIndex                                   = 0    //used at the secondary index for the client
  private var updatesSinceLastIDChange                           = 0    //used to know how many messages to wait for when blocking in the Q manager
  private var newIDRequiredOnUpdate                              = true // has a query been since the last update and do I need a new ID
  private var currentSourceID                                    = -1   //this is initialised as soon as the client sends 1 update

  def IDForUpdates(): Int = {
    if (newIDRequiredOnUpdate)
      idManager.getNextAvailableID() match {
        case Some(id) =>
          currentSourceID = id
          newIDRequiredOnUpdate = false
        case None     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID")
      } //updates the sourceID if we haven't had one yet or if the user has sent a query since the last update block
    currentSourceID
  }

  def getIndex: Int = totalUpdateIndex

  def submit(query: Query, customJobName: String = ""): QueryProgressTracker = {

    if (updatesSinceLastIDChange > 0) { //TODO Think this will block multi-client -- not an issue for right now
      unblockIngestion(sourceID = currentSourceID, updatesSinceLastIDChange, force = false)
      blockingSources += currentSourceID
      updatesSinceLastIDChange = 0
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

  def individualUpdate(update: GraphUpdate): Unit = {
    latestMsgTimeToFlushToFlight = System.currentTimeMillis()
    writers(Math.abs((update.srcId % totalPartitions).toInt)) sendAsync update
    totalUpdateIndex += 1
    updatesSinceLastIDChange += 1
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

  def closeArrow(): Unit = writers.values.foreach(_.close())

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
