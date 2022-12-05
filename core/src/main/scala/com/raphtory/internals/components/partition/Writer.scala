package com.raphtory.internals.components.partition

import com.raphtory.internals.graph.GraphAlteration._
import cats.effect.Async
import cats.effect.Resource
import cats.effect.std.Semaphore
import cats.syntax.all._
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol
import com.raphtory.protocol.PartitionService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.language.postfixOps

private[raphtory] class Writer[F[_]](
//    lock: Semaphore[F],
    graphID: String,
    partitionID: Int,
    storage: GraphPartition
//    partitions: Map[Int, PartitionService[F]],
//    conf: Config
)(implicit F: Async[F]) {

  private val vertexAdditionCount = TelemetryReporter.writerVertexAdditions.labels(partitionID.toString, graphID)
  private val edgeAddCount        = TelemetryReporter.writerEdgeAdditions.labels(partitionID.toString, graphID)
  private val edgeRemovalCount    = TelemetryReporter.writerEdgeDeletions.labels(partitionID.toString, graphID)
  private val vertexRemoveCount   = TelemetryReporter.writerVertexDeletions.labels(partitionID.toString, graphID)

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val partitioner = Partitioner()

  def processUpdates(req: protocol.GraphAlterations): F[Unit] =
    Async[F].blocking(handleLocalAlterations(req))

//  def processEffects(req: protocol.GraphAlterations): F[Unit] =
//    for {
//      _ <- processAlterations(req)
//    } yield ()
//
//  private def processAlterations(req: protocol.GraphAlterations): F[Unit] =
//    for {
//      effects <- Async[F].blocking(handleLocalAlterations(req))
//      delivery = effects
//                   .groupBy(effect => partitioner.getPartitionForId(effect.updateId))
//                   .map {
//                     case (partitionId, effects) =>
//                       partitions(partitionId).processEffects(
//                               protocol.GraphAlterations(graphID, effects.map(protocol.GraphAlteration(_)))
//                       )
//                   }
//                   .toVector
//      _       <- Async[F]
//                   .parSequenceN(partitioner.totalPartitions)(delivery)
//                   .void
//    } yield ()

  private def handleLocalAlterations(msgs: protocol.GraphAlterations): Unit =
    msgs.alterations.view
      .map(_.alteration)
      .foreach {
        //Updates from the Graph Builder
        case update: VertexAdd => processVertexAdd(update)
        case update: EdgeAdd   => processEdgeAdd(update)
//        case update: EdgeDelete                   => processEdgeDelete(update)
//        case update: VertexDelete                 =>
//          processVertexDelete(update) //Delete a vertex and all associated edges

//        //Syncing Edge Additions
//        case update: SyncNewEdgeAdd               =>
//          processSyncNewEdgeAdd(
//                  update
//          ) //A writer has requested a new edge sync for a destination node in this worker
//        case update: SyncExistingEdgeAdd          =>
//          processSyncExistingEdgeAdd(
//                  update
//          ) // A writer has requested an existing edge sync for a destination node on in this worker
//
//        //Syncing Edge Removals
//        case update: SyncNewEdgeRemoval           =>
//          processSyncNewEdgeRemoval(
//                  update
//          ) //A remote worker is asking for a new edge to be removed for a destination node in this worker
//
//        case update: SyncExistingEdgeRemoval      =>
//          processSyncExistingEdgeRemoval(
//                  update
//          ) //A remote worker is asking for the deletion of an existing edge
//
//        //Syncing Vertex Removals
//        case update: OutboundEdgeRemovalViaVertex =>
//          processOutboundEdgeRemovalViaVertex(
//                  update
//          ) //Syncs the deletion of an edge, but for when the removal comes from a vertex
//        case update: InboundEdgeRemovalViaVertex  => processInboundEdgeRemovalViaVertex(update)
//
//        //Response from storing the destination node being synced
//        case update: SyncExistingRemovals =>
//          processSyncExistingRemovals(
//                  update
//          ) //The remote worker has returned all removals in the destination node -- for new edges

        case other             =>
          logger.error(s"Partition '$partitionID': Received unsupported message type '$other'.")
          throw new IllegalStateException(
                  s"Partition '$partitionID': Received unsupported message '$other'."
          )
      }
//      .toVector

  // TODO: bring back `processedMessages += 1`

  // Graph Updates from the builders
  def processVertexAdd(update: VertexAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    storage.addVertex(update.sourceID, update.updateTime, update.index, update.srcId, update.properties, update.vType)
    vertexAdditionCount.inc()
  }

  def processEdgeAdd(update: EdgeAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeAdd message '$update'.")

    storage.addEdge(
            update.sourceID,
            update.updateTime,
            update.index,
            update.srcId,
            update.dstId,
            update.properties,
            update.eType
    )
    edgeAddCount.inc()
  }

//  def processEdgeDelete(update: EdgeDelete): List[GraphUpdateEffect] = {
//    logger.trace(s"Partition $partitionID: Received EdgeDelete message '$update'.")
//
//    val effect = storage.removeEdge(update.sourceID, update.updateTime, update.index, update.srcId, update.dstId)
//    edgeRemovalCount.inc()
//    effect.toList
//  }

//  def processVertexDelete(update: VertexDelete): List[GraphUpdateEffect] = {
//    logger.trace(s"Partition $partitionID: Received VertexDelete message '$update'.")
//
//    val edgeRemovals = storage.removeVertex(update.sourceID, update.updateTime, update.index, update.srcId)
//    vertexRemoveCount.inc()
//    edgeRemovals
//  }

  // Graph Effects for syncing edge adds
//  def processSyncNewEdgeAdd(req: SyncNewEdgeAdd): List[GraphUpdateEffect] = {
//    logger.trace("A writer has requested a new edge sync for a destination node in this worker.")
//
//    val effect = storage
//      .addIncomingEdge(
//              req.sourceID,
//              req.updateTime,
//              req.index,
//              req.srcId,
//              req.dstId,
//              req.properties,
//              req.removals,
//              req.vType
//      )
//    List(effect)
//  }

//  def processSyncExistingEdgeAdd(req: SyncExistingEdgeAdd): List[GraphUpdateEffect] = {
//    logger.trace(
//            s"Partition '$partitionID': A writer has requested an existing edge sync for a destination node on in this worker."
//    )
//    storage.syncExistingEdgeAdd(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.properties)
//    List()
//  }

  // Graph Effects for syncing edge deletions
//  def processSyncNewEdgeRemoval(req: SyncNewEdgeRemoval): List[GraphUpdateEffect] = {
//    logger.trace(
//            s"Partition '$partitionID': A remote worker is asking for a new edge to be removed for a destination node in this worker."
//    )
//    val effect = storage.syncNewEdgeRemoval(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId, req.removals)
//    List(effect)
//  }

//  def processSyncExistingEdgeRemoval(req: SyncExistingEdgeRemoval): List[GraphUpdateEffect] = {
//    logger.trace(s"Partition '$partitionID': A remote worker is asking for the deletion of an existing edge.")
//    storage.syncExistingEdgeRemoval(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
//    List()
//  }

  // Graph Effects for syncing vertex deletions
//  def processOutboundEdgeRemovalViaVertex(req: OutboundEdgeRemovalViaVertex): List[GraphUpdateEffect] = {
//    logger.trace(
//            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes from a vertex."
//    )
//    storage.outboundEdgeRemovalViaVertex(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
//    List()
//  }

//  def processInboundEdgeRemovalViaVertex(req: InboundEdgeRemovalViaVertex): List[GraphUpdateEffect] = { //remote worker same as above
//    logger.trace(
//            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes to a vertex."
//    )
//    storage.inboundEdgeRemovalViaVertex(req.sourceID, req.updateTime, req.index, req.srcId, req.dstId)
//    List()
//  }

  // Responses from the secondary server
//  def processSyncExistingRemovals(req: SyncExistingRemovals): List[GraphUpdateEffect] = { //when the new edge add is responded to we can say it is synced
//    logger.trace(
//            s"Partition '$partitionID': The remote worker has returned all removals in the destination node -- for new edges"
//    )
//
//    storage.syncExistingRemovals(req.updateTime, req.index, req.srcId, req.dstId, req.removals)
//    List()
//  }
}

object Writer {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def apply[F[_]: Async](
      graphID: String,
      partitionID: Int,
      storage: GraphPartition,
      partitions: Map[Int, PartitionService[F]],
      conf: Config
  ): Resource[F, Writer[F]] =
    for {
      _      <- Resource.eval(startupMessage(graphID, partitionID))
      writer <- Resource.eval(Async[F].delay(new Writer[F](graphID, partitionID, storage)))
    } yield writer

  private def startupMessage[F[_]: Async](graphId: String, partitionId: Int) =
    Async[F].delay(logger.debug(s"Starting writer for graph '$graphId' and partition '$partitionId'"))
}
