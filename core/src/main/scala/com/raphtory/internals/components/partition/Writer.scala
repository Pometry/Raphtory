package com.raphtory.internals.components.partition

import com.raphtory.internals.graph.GraphAlteration._
import cats.effect.kernel.Spawn
import cats.effect.Async
import cats.effect.Resource
import com.raphtory.api.input._
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.control.NonFatal

private[raphtory] class Writer(
    graphID: String,
    partitionID: Int,
    storage: GraphPartition,
    conf: Config,
    topics: TopicRepository,
    override val scheduler: Scheduler
) extends Component[GraphAlteration](conf)
        with FlushToFlight {

  override val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private lazy val neighbours =
    topics.graphSync(graphID).endPoint() //This needs to be lazy otherwise the zookeeper lookup with deadlock

  override lazy val writers: Map[Int, EndPoint[GraphUpdateEffect]] = neighbours

  override def run(): Unit = {}

  override def stop(): Unit = {
    close()
    neighbours.values.foreach(_.close())
  }

  override def handleMessage(msg: GraphAlteration): Unit = {
    latestMsgTimeToFlushToFlight = System.currentTimeMillis()

    try msg match {
      //Updates from the Graph Builder
      case update: VertexAdd                    => processVertexAdd(update)
      case update: EdgeAdd                      => processEdgeAdd(update)
      case update: EdgeDelete                   => processEdgeDelete(update)
      case update: VertexDelete                 =>
        processVertexDelete(update) //Delete a vertex and all associated edges

      //Syncing Edge Additions
      case update: SyncNewEdgeAdd               =>
        processSyncNewEdgeAdd(
                update
        ) //A writer has requested a new edge sync for a destination node in this worker
      case update: SyncExistingEdgeAdd          =>
        processSyncExistingEdgeAdd(
                update
        ) // A writer has requested an existing edge sync for a destination node on in this worker

      //Syncing Edge Removals
      case update: SyncNewEdgeRemoval           =>
        processSyncNewEdgeRemoval(
                update
        ) //A remote worker is asking for a new edge to be removed for a destination node in this worker

      case update: SyncExistingEdgeRemoval      =>
        processSyncExistingEdgeRemoval(
                update
        ) //A remote worker is asking for the deletion of an existing edge

      //Syncing Vertex Removals
      case update: OutboundEdgeRemovalViaVertex =>
        processOutboundEdgeRemovalViaVertex(
                update
        ) //Syncs the deletion of an edge, but for when the removal comes from a vertex
      case update: InboundEdgeRemovalViaVertex  => processInboundEdgeRemovalViaVertex(update)

      //Response from storing the destination node being synced
      case update: SyncExistingRemovals =>
        processSyncExistingRemovals(
                update
        ) //The remote worker has returned all removals in the destination node -- for new edges

      case update: EdgeSyncAck          =>
        processEdgeSyncAck(update) //The remote worker acknowledges the completion of an edge sync
      case update: VertexRemoveSyncAck  => processVertexRemoveSyncAck(update)

      case other =>
        logger.error(s"Partition '$partitionID': Received unsupported message type '$other'.")
        throw new IllegalStateException(
                s"Partition '$partitionID': Received unsupported message '$other'."
        )
    }
    catch {
      case NonFatal(e) =>
        logger.error(s"Failed to handle message $msg", e)
    }

    handleUpdateCount()
  }

  // Graph Updates from the builders
  def processVertexAdd(update: VertexAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    storage.addVertex(update.sourceID, update.updateTime, update.index, update.srcId, update.properties, update.vType)
    storage.timings(update.updateTime)
    storage.watermarker.safeRecordCompletedUpdate(update.sourceID)
    telemetry.vertexAddCollector
      .labels(partitionID.toString, graphID)
      .inc()

  }

  def processEdgeAdd(update: EdgeAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeAdd message '$update'.")

    storage.timings(update.updateTime)
    storage.addEdge(
            update.sourceID,
            update.updateTime,
            update.index,
            update.srcId,
            update.dstId,
            update.properties,
            update.eType
    ) match {
      case Some(value) =>
        neighbours(getWriter(value.updateId)) sendAsync value
        storage.watermarker.trackEdgeAddition(update.updateTime, update.index, update.srcId, update.dstId)
      case None        => storage.watermarker.safeRecordCompletedUpdate(update.sourceID)

    }
    telemetry.streamWriterEdgeAdditionsCollector.labels(partitionID.toString, graphID).inc()
  }

  def processEdgeDelete(update: EdgeDelete): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeDelete message '$update'.")

    storage.timings(update.updateTime)
    storage.removeEdge(update.sourceID, update.updateTime, update.index, update.srcId, update.dstId) match {
      case Some(value) =>
        neighbours(getWriter(value.updateId)) sendAsync value
        storage.watermarker.trackEdgeDeletion(update.updateTime, update.index, update.srcId, update.dstId)
      case None        => storage.watermarker.safeRecordCompletedUpdate(update.sourceID)

    }
    telemetry.streamWriterEdgeDeletionsCollector.labels(partitionID.toString, graphID).inc()
  }

  def processVertexDelete(update: VertexDelete): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexDelete message '$update'.")

    val edgeRemovals = storage.removeVertex(update.sourceID, update.updateTime, update.index, update.srcId)
    if (edgeRemovals.nonEmpty) {
      edgeRemovals.foreach(effect => neighbours(getWriter(effect.updateId)) sendAsync effect)
      storage.watermarker.trackVertexDeletion(update.updateTime, update.index, update.srcId, edgeRemovals.size)
    }
    telemetry.streamWriterVertexDeletionsCollector
      .labels(partitionID.toString, graphID)
      .inc()
  }

  // Graph Effects for syncing edge adds
  def processSyncNewEdgeAdd(req: SyncNewEdgeAdd): Unit = {
    logger.trace("A writer has requested a new edge sync for a destination node in this worker.")

    storage.timings(req.updateTime)
    val effect = storage
      .syncNewEdgeAdd(
              req.sourceID,
              req.updateTime,
              req.index,
              req.srcId,
              req.dstId,
              req.properties,
              req.removals,
              req.vType
      )
    neighbours(getWriter(effect.updateId)) sendAsync effect
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  def processSyncExistingEdgeAdd(req: SyncExistingEdgeAdd): Unit = {
    logger.trace(
            s"Partition '$partitionID': A writer has requested an existing edge sync for a destination node on in this worker."
    )

    storage.timings(req.updateTime)
    val effect =
      storage.syncExistingEdgeAdd(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.properties)
    neighbours(getWriter(effect.updateId)) sendAsync effect
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  // Graph Effects for syncing edge deletions
  def processSyncNewEdgeRemoval(req: SyncNewEdgeRemoval): Unit = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for a new edge to be removed for a destination node in this worker."
    )

    storage.timings(req.updateTime)
    val effect = storage.syncNewEdgeRemoval(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.removals)
    neighbours(getWriter(effect.updateId)) sendAsync effect
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  def processSyncExistingEdgeRemoval(req: SyncExistingEdgeRemoval): Unit = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for the deletion of an existing edge."
    )

    storage.timings(req.updateTime)
    val effect = storage.syncExistingEdgeRemoval(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
    neighbours(getWriter(effect.updateId)) sendAsync effect
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  // Graph Effects for syncing vertex deletions
  def processOutboundEdgeRemovalViaVertex(req: OutboundEdgeRemovalViaVertex): Unit = {
    logger.trace(
            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes from a vertex."
    )

    storage.timings(req.updateTime)
    val effect = storage.outboundEdgeRemovalViaVertex(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
    neighbours(getWriter(effect.updateId)) sendAsync effect
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  def processInboundEdgeRemovalViaVertex(req: InboundEdgeRemovalViaVertex): Unit = { //remote worker same as above
    logger.trace(
            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes to a vertex."
    )

    val effect = storage.inboundEdgeRemovalViaVertex(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
    neighbours(getWriter(effect.updateId)) sendAsync effect
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  // Responses from the secondary server
  def processSyncExistingRemovals(req: SyncExistingRemovals): Unit = { //when the new edge add is responded to we can say it is synced
    logger.trace(
            s"Partition '$partitionID': The remote worker has returned all removals in the destination node -- for new edges"
    )

    storage.syncExistingRemovals(req.updateTime, req.index, req.srcId, req.dstId, req.removals)
    untrackEdgeUpdate(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.fromAddition)
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  def processEdgeSyncAck(req: EdgeSyncAck): Unit = {
    logger.trace(
            s"Partition '$partitionID': The remote worker acknowledges the completion of an edge sync."
    )

    untrackEdgeUpdate(
            req.sourceID,
            req.updateTime,
            req.index,
            req.srcId,
            req.dstId,
            req.fromAddition
    ) //when the edge isn't new we will get this response instead
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  private def untrackEdgeUpdate(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      fromAddition: Boolean
  ): Unit =
    if (fromAddition)
      storage.watermarker.untrackEdgeAddition(sourceID, msgTime, index, srcId, dstId)
    else
      storage.watermarker.untrackEdgeDeletion(sourceID, msgTime, index, srcId, dstId)

  def processVertexRemoveSyncAck(req: VertexRemoveSyncAck): Unit = {
    logger.trace(
            s"Partition '$partitionID': The remote worker acknowledges the completion of vertex removal."
    )

    storage.watermarker.untrackVertexDeletion(req.sourceID, req.updateTime, req.index, req.updateId)
    telemetry.totalSyncedStreamWriterUpdatesCollector.labels(partitionID.toString, graphID)
  }

  def handleUpdateCount(): Unit = {
    processedMessages += 1
    telemetry.streamWriterGraphUpdatesCollector.labels(partitionID.toString, graphID).inc()
  }

}

object Writer {

  def apply[IO[_]: Async](
      graphID: String,
      partitionId: Int,
      storage: GraphPartition,
      config: Config,
      topics: TopicRepository,
      scheduler: Scheduler
  ): Resource[IO, Writer] =
    Component.makeAndStartPart(
            partitionId,
            topics,
            s"writer-$partitionId",
            List(topics.graphUpdates(graphID), topics.graphSync(graphID)),
            new Writer(
                    graphID = graphID,
                    partitionID = partitionId,
                    storage = storage,
                    conf = config,
                    topics = topics,
                    scheduler
            )
    )

}
