package com.raphtory.internals.components.partition

import cats.Parallel
import com.raphtory.internals.graph.GraphAlteration._
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Resource
import cats.effect.std.Queue
import cats.syntax.all._
import com.raphtory.api.input._
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.GrpcServiceDescriptor
import com.raphtory.internals.components.ServiceDescriptor
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol
import com.raphtory.protocol.Empty
import com.raphtory.protocol.PartitionService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.control.NonFatal

private[raphtory] class Writer[F[_]](
    graphID: String,
    partitionID: Int,
    storage: GraphPartition,
    partitions: Map[Int, PartitionService[F]],
    conf: Config
)(implicit F: Async[F]) {

  private val vertexAdditionCount = TelemetryReporter.writerVertexAdditions.labels(partitionID.toString, graphID)
  private val edgeAddCount        = TelemetryReporter.writerEdgeAdditions.labels(partitionID.toString, graphID)
  private val edgeRemovalCount    = TelemetryReporter.writerEdgeDeletions.labels(partitionID.toString, graphID)
  private val vertexRemoveCount   = TelemetryReporter.writerVertexDeletions.labels(partitionID.toString, graphID)

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val partitioner = Partitioner()

  def processAlterations(req: protocol.GraphAlterations): F[Empty] =
    for {
      effects <- Async[F].blocking(handleLocalAlterations(req))
      delivery = effects
                   .groupBy(effect => partitioner.getPartitionForId(effect.updateId))
                   .map {
                     case (partitionId, effects) =>
                       partitions(partitionId).processAlterations(
                               protocol.GraphAlterations(graphID, effects.map(protocol.GraphAlteration(_)))
                       )
                   }
                   .toVector
      _       <- Async[F]
                   .parSequenceN(partitioner.totalPartitions)(delivery)
                   .void
    } yield Empty()

  private def handleLocalAlterations(msgs: protocol.GraphAlterations): Vector[GraphUpdateEffect] =
    msgs.alterations.view
      .map(_.alteration)
      .flatMap {
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
      .toVector

  // TODO: bring back `processedMessages += 1`

  // Graph Updates from the builders
  def processVertexAdd(update: VertexAdd): List[GraphUpdateEffect] = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    storage.addVertex(update.sourceID, update.updateTime, update.index, update.srcId, update.properties, update.vType)
    storage.timings(update.updateTime)
    storage.watermarker.safeRecordCompletedUpdate(update.sourceID)
    vertexAdditionCount.inc()
    List()
  }

  def processEdgeAdd(update: EdgeAdd): List[GraphUpdateEffect] = {
    logger.trace(s"Partition $partitionID: Received EdgeAdd message '$update'.")

    storage.timings(update.updateTime)
    val effect = storage.addEdge(
            update.sourceID,
            update.updateTime,
            update.index,
            update.srcId,
            update.dstId,
            update.properties,
            update.eType
    )
    effect match {
      case Some(value) =>
        storage.watermarker.trackEdgeAddition(update.updateTime, update.index, update.srcId, update.dstId)
      case None        => storage.watermarker.safeRecordCompletedUpdate(update.sourceID)

    }
    edgeAddCount.inc()
    effect.toList
  }

  def processEdgeDelete(update: EdgeDelete): List[GraphUpdateEffect] = {
    logger.trace(s"Partition $partitionID: Received EdgeDelete message '$update'.")

    storage.timings(update.updateTime)
    val effect = storage.removeEdge(update.sourceID, update.updateTime, update.index, update.srcId, update.dstId)
    effect match {
      case Some(value) =>
        storage.watermarker.trackEdgeDeletion(update.updateTime, update.index, update.srcId, update.dstId)
      case None        => storage.watermarker.safeRecordCompletedUpdate(update.sourceID)

    }
    edgeRemovalCount.inc()
    effect.toList
  }

  def processVertexDelete(update: VertexDelete): List[GraphUpdateEffect] = {
    logger.trace(s"Partition $partitionID: Received VertexDelete message '$update'.")

    val edgeRemovals = storage.removeVertex(update.sourceID, update.updateTime, update.index, update.srcId)
    if (edgeRemovals.nonEmpty)
      storage.watermarker.trackVertexDeletion(update.updateTime, update.index, update.srcId, edgeRemovals.size)
    vertexRemoveCount.inc()
    edgeRemovals
  }

  // Graph Effects for syncing edge adds
  def processSyncNewEdgeAdd(req: SyncNewEdgeAdd): List[GraphUpdateEffect] = {
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
    List(effect)
  }

  def processSyncExistingEdgeAdd(req: SyncExistingEdgeAdd): List[GraphUpdateEffect] = {
    logger.trace(
            s"Partition '$partitionID': A writer has requested an existing edge sync for a destination node on in this worker."
    )

    storage.timings(req.updateTime)
    val effect =
      storage.syncExistingEdgeAdd(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.properties)
    List(effect)
  }

  // Graph Effects for syncing edge deletions
  def processSyncNewEdgeRemoval(req: SyncNewEdgeRemoval): List[GraphUpdateEffect] = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for a new edge to be removed for a destination node in this worker."
    )

    storage.timings(req.updateTime)
    val effect = storage.syncNewEdgeRemoval(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.removals)
    List(effect)
  }

  def processSyncExistingEdgeRemoval(req: SyncExistingEdgeRemoval): List[GraphUpdateEffect] = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for the deletion of an existing edge."
    )

    storage.timings(req.updateTime)
    val effect = storage.syncExistingEdgeRemoval(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
    List(effect)
  }

  // Graph Effects for syncing vertex deletions
  def processOutboundEdgeRemovalViaVertex(req: OutboundEdgeRemovalViaVertex): List[GraphUpdateEffect] = {
    logger.trace(
            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes from a vertex."
    )

    storage.timings(req.updateTime)
    val effect = storage.outboundEdgeRemovalViaVertex(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
    List(effect)
  }

  def processInboundEdgeRemovalViaVertex(req: InboundEdgeRemovalViaVertex): List[GraphUpdateEffect] = { //remote worker same as above
    logger.trace(
            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes to a vertex."
    )

    val effect = storage.inboundEdgeRemovalViaVertex(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
    List(effect)
  }

  // Responses from the secondary server
  def processSyncExistingRemovals(req: SyncExistingRemovals): List[GraphUpdateEffect] = { //when the new edge add is responded to we can say it is synced
    logger.trace(
            s"Partition '$partitionID': The remote worker has returned all removals in the destination node -- for new edges"
    )

    storage.syncExistingRemovals(req.updateTime, req.index, req.srcId, req.dstId, req.removals)
    untrackEdgeUpdate(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.fromAddition)
    List()
  }

  def processEdgeSyncAck(req: EdgeSyncAck): List[GraphUpdateEffect] = {
    logger.trace(s"Partition '$partitionID': The remote worker acknowledges the completion of an edge sync.")
    untrackEdgeUpdate(
            req.sourceID,
            req.updateTime,
            req.index,
            req.srcId,
            req.dstId,
            req.fromAddition
    ) //when the edge isn't new we will get this response instead
    List()
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

  def processVertexRemoveSyncAck(req: VertexRemoveSyncAck): List[GraphUpdateEffect] = {
    logger.trace(
            s"Partition '$partitionID': The remote worker acknowledges the completion of vertex removal."
    )
    storage.watermarker.untrackVertexDeletion(req.sourceID, req.updateTime, req.index, req.updateId)
    List()
  }
}

object Writer {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](
      graphID: String,
      partitionID: Int,
      storage: GraphPartition,
      registry: ServiceRegistry[F],
      conf: Config
  ): Resource[F, Writer[F]] =
    for {
      _          <- Resource.eval(startupMessage(graphID, partitionID))
      partitions <- registry.partitions
      writer     <- Resource.eval(Async[F].delay(new Writer[F](graphID, partitionID, storage, partitions, conf)))
    } yield writer

  private def startupMessage[F[_]: Async](graphId: String, partitionId: Int) =
    Async[F].delay(logger.debug(s"Starting writer for graph '$graphId' and partition '$partitionId'"))
}
