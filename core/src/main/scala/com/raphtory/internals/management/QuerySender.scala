package com.raphtory.internals.management

import com.raphtory.api.input.Graph
import com.raphtory.api.analysis.table.TableOutputTracker
import com.raphtory.api.input.Source
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.management.id.IDManager
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.Random

private[raphtory] class QuerySender(
    val graphID: String,
    override val scheduler: Scheduler,
    private val topics: TopicRepository,
    private val config: Config,
    private val idManager: IDManager,
    private val clientID: String
) extends Graph
        with FlushToFlight {

  class NoIDException(message: String) extends Exception(message)

  override val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  override lazy val writers            = topics.graphUpdates(graphID).endPoint
  private lazy val queryManager        = topics.blockingIngestion(graphID).endPoint
  private lazy val graphSetup          = topics.graphSetup.endPoint
  private lazy val submissions         = topics.submissions(graphID).endPoint
  private val blockingSources          = ArrayBuffer[Long]()
  private var highestTimeSeen          = Long.MinValue
  private var totalUpdateIndex         = 0    //used at the secondary index for the client
  private var updatesSinceLastIDChange = 0    //used to know how many messages to wait for when blocking in the Q manager
  private var newIDRequiredOnUpdate    = true // has a query been since the last update and do I need a new ID
  private var currentSourceID          = -1   //this is initialised as soon as the client sends 1 update
  private var searchPath               = List.empty[String]

  def addToDynamicPath(name: String): Unit = searchPath = name :: searchPath

  override protected def sourceID: Int = IDForUpdates()
  override def index: Long             = totalUpdateIndex

  def IDForUpdates(): Int = {
    if (newIDRequiredOnUpdate)
      idManager.getNextAvailableID(graphID) match {
        case Some(id) =>
          currentSourceID = id
          newIDRequiredOnUpdate = false
        case None     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID")
      } //updates the sourceID if we haven't had one yet or if the user has sent a query since the last update block
    currentSourceID
  }

  def handleInternal(update: GraphUpdate): Unit =
    handleGraphUpdate(update) //Required so the Temporal Graph obj can call the below func

  override protected def handleGraphUpdate(update: GraphUpdate): Unit = {
    latestMsgTimeToFlushToFlight = System.currentTimeMillis()
    highestTimeSeen = highestTimeSeen max update.updateTime
    writers(getPartitionForId(update.srcId)) sendAsync update
    totalUpdateIndex += 1
    updatesSinceLastIDChange += 1

  }

  def submit(query: Query, customJobName: String = ""): String = {

    if (updatesSinceLastIDChange > 0) { //TODO Think this will block multi-client -- not an issue for right now
      unblockIngestion(sourceID = currentSourceID, updatesSinceLastIDChange, force = false)
      blockingSources += currentSourceID
      updatesSinceLastIDChange = 0
      newIDRequiredOnUpdate = true
    }

    val jobName     = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID       = jobName + "_" + Random.nextLong().abs
    val outputQuery = query
      .copy(
              name = jobID,
              blockedBy = blockingSources.toArray,
              _bootstrap = query._bootstrap.resolve(searchPath)
      )

    submissions sendAsync outputQuery
    jobID
  }

  def createTracker(jobID: String): QueryProgressTracker = {
    val tracker = QueryProgressTracker.unsafeApply(graphID, jobID, config, topics)
    scheduler.execute(tracker)
    tracker
  }

  private def unblockIngestion(sourceID: Int, index: Long, force: Boolean): Unit =
    queryManager.sendAsync(UnblockIngestion(sourceID, graphID = graphID, index, highestTimeSeen, force))

  def destroyGraph(force: Boolean): Unit = graphSetup sendAsync DestroyGraph(graphID, clientID, force)

  def disconnect(): Unit = graphSetup sendAsync ClientDisconnected(graphID, clientID)

  def establishGraph(): Unit = graphSetup sendAsync EstablishGraph(graphID, clientID)

  def outputCollector(jobID: String, timeout: Duration): TableOutputTracker = {
    val collector = TableOutputTracker(graphID, jobID, topics, config, timeout)
    scheduler.execute(collector)
    collector
  }

  def submitSource(blocking: Boolean, sources: Seq[Source], id: String): Unit = {

    val clazzes      = sources.map { source =>
      source.getBuilderClass
    }.toList
    val sourceWithId = sources.map { source =>
      idManager.getNextAvailableID(graphID) match {
        case Some(id) =>
          if (blocking) blockingSources += id
          (id, source)
        case None     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID for $source")
      }
    }
    submissions sendAsync IngestData(
            DynamicLoader(clazzes).resolve(searchPath),
            graphID,
            id,
            sourceWithId,
            blocking
    )
  }

  def closeArrow(): Unit = writers.values.foreach(_.close())

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
