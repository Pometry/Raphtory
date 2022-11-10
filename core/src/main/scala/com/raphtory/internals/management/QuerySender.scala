package com.raphtory.internals.management

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Source
import com.raphtory.api.progresstracker.ProgressTracker
import com.raphtory.api.progresstracker.QueryProgressTracker
import com.raphtory.api.progresstracker.QueryProgressTrackerWithIterator
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.output.OutputMessages
import com.raphtory.internals.components.querymanager._
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.protocol
import com.raphtory.protocol.GraphInfo
import com.raphtory.protocol.RaphtoryService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.Random
import scala.util.Success

private[raphtory] class QuerySender(
    val graphID: String,
    private val service: RaphtoryService[IO],
    override val scheduler: Scheduler,
    private val topics: TopicRepository,
    private val config: Config,
    private val clientID: String
) extends Graph
        with FlushToFlight {

  class NoIDException(message: String) extends Exception(message)

  override val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  override lazy val writers: Map[Int, EndPoint[_]] = topics.graphUpdates(graphID).endPoint()  // TODO Rid this when topic repository is thrashed
  private val blockingSources                      = ArrayBuffer[Long]()
  private var highestTimeSeen                      = Long.MinValue
  private var totalUpdateIndex                     = 0    //used at the secondary index for the client
  private var updatesSinceLastIDChange             = 0    //used to know how many messages to wait for when blocking in the Q manager
  private var newIDRequiredOnUpdate                = true // has a query been since the last update and do I need a new ID
  private var currentSourceID                      = -1   //this is initialised as soon as the client sends 1 update
  private var searchPath                           = List.empty[String]

  def addToDynamicPath(name: String): Unit = searchPath = name :: searchPath

  override protected def sourceID: Int = IDForUpdates()
  override def index: Long             = totalUpdateIndex

  def IDForUpdates(): Int = {
    if (newIDRequiredOnUpdate)
      service.getNextAvailableId(protocol.IdPool(graphID)).unsafeRunSync() match {
        case protocol.OptionalId(Some(id), _) =>
          currentSourceID = id
          newIDRequiredOnUpdate = false
        case protocol.OptionalId(None, _)     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID")
      } //updates the sourceID if we haven't had one yet or if the user has sent a query since the last update block
    currentSourceID
  }

  def handleInternal(update: GraphUpdate): Unit =
    handleGraphUpdate(update) // Required so the Temporal Graph obj can call the below func

  override protected def handleGraphUpdate(update: GraphUpdate): Unit = {
    latestMsgTimeToFlushToFlight = System.currentTimeMillis()
    highestTimeSeen = highestTimeSeen max update.updateTime
    service.processUpdate(protocol.GraphUpdate(graphID, update)).unsafeRunSync()
    totalUpdateIndex += 1
    updatesSinceLastIDChange += 1
  }

  def submit(
      query: Query,
      customJobName: String = "",
      createProgressTracker: String => ProgressTracker
  ): ProgressTracker = {

    val jobName = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID   = jobName + "_" + Random.nextLong().abs

    val progressTracker: ProgressTracker = createProgressTracker(jobID)

    if (updatesSinceLastIDChange > 0) { // TODO Think this will block multi-client -- not an issue for right now
      unblockIngestion(sourceID = currentSourceID, updatesSinceLastIDChange, force = false)
      blockingSources += currentSourceID
      updatesSinceLastIDChange = 0
      newIDRequiredOnUpdate = true
    }

    val outputQuery = query
      .copy(
              name = jobID,
              blockedBy = blockingSources.toArray,
              _bootstrap = query._bootstrap.resolve(searchPath)
      )

    val responses = service.submitQuery(protocol.Query(TryQuery(Success(outputQuery)))).unsafeRunSync()
    responses
      .map(_.bytes)
      .foreach {
        case message: OutputMessages =>
          IO(progressTracker.asInstanceOf[QueryProgressTrackerWithIterator].handleOutputMessage(message))
        case message                 =>
          IO(progressTracker.handleMessage(message))
      }
      .compile
      .drain
      .unsafeRunAndForget()

    progressTracker
  }

  def createQueryProgressTracker(jobID: String): QueryProgressTracker =
    QueryProgressTracker(graphID, jobID, config)

  def createTableOutputTracker(jobID: String, timeout: Duration): QueryProgressTrackerWithIterator =
    QueryProgressTrackerWithIterator(graphID, jobID, topics, config, timeout)

  private def unblockIngestion(sourceID: Int, messageCount: Long, force: Boolean): Unit =
    service
      .unblockIngestion(protocol.UnblockIngestion(graphID, sourceID, messageCount, highestTimeSeen))
      .unsafeRunSync()

  def destroyGraph(force: Boolean): Unit =
    service.destroyGraph(protocol.DestroyGraph(clientID, graphID, force)).unsafeRunSync()

  def disconnect(): Unit = service.disconnect(GraphInfo(clientID, graphID)).unsafeRunSync()

  def establishGraph(): Unit = service.establishGraph(GraphInfo(clientID, graphID)).unsafeRunSync()

  def submitSources(blocking: Boolean, sources: Seq[Source]): Unit = {
    val clazzes      = sources.map { source =>
      source.getBuilderClass
    }.toList
    val sourceWithId = sources.map { source =>
      service.getNextAvailableId(protocol.IdPool(graphID)).unsafeRunSync() match {
        case protocol.OptionalId(Some(id), _) =>
          if (blocking) blockingSources += id
          (id, source)
        case protocol.OptionalId(None, _)     =>
          throw new NoIDException(s"Client '$clientID' was not able to acquire a source ID for $source")
      }
    }

    sourceWithId foreach {
      case (id, source) =>
        val ingestData = IngestData(DynamicLoader(clazzes).resolve(searchPath), graphID, id, source)
        service.submitSource(protocol.IngestData(TryIngestData(Success(ingestData)))).unsafeRunSync()
    }
  }

  def closeArrow(): Unit = writers.values.foreach(_.close())

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
