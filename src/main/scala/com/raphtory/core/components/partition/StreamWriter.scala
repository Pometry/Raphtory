package com.raphtory.core.components.partition

import com.raphtory.core.components.Component
import com.raphtory.core.components.graphbuilder.EdgeAdd
import com.raphtory.core.components.graphbuilder.EdgeDelete
import com.raphtory.core.components.graphbuilder.EdgeSyncAck
import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.components.graphbuilder.GraphUpdate
import com.raphtory.core.components.graphbuilder.InboundEdgeRemovalViaVertex
import com.raphtory.core.components.graphbuilder.OutboundEdgeRemovalViaVertex
import com.raphtory.core.components.graphbuilder.SyncExistingEdgeAdd
import com.raphtory.core.components.graphbuilder.SyncExistingEdgeRemoval
import com.raphtory.core.components.graphbuilder.SyncExistingRemovals
import com.raphtory.core.components.graphbuilder.SyncNewEdgeAdd
import com.raphtory.core.components.graphbuilder.SyncNewEdgeRemoval
import com.raphtory.core.components.graphbuilder.VertexAdd
import com.raphtory.core.components.graphbuilder.VertexDelete
import com.raphtory.core.components.graphbuilder.VertexRemoveSyncAck
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph._
import com.typesafe.config.Config
import com.raphtory.core.config.telemetry.PartitionTelemetry
import io.prometheus.client.Summary
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import java.util.Calendar
import scala.collection.mutable
import scala.language.postfixOps

/** @DoNotDocument */
class StreamWriter(
    partitionID: Int,
    storage: GraphPartition,
    conf: Config,
    pulsarController: PulsarController
) extends Component[GraphAlteration](conf: Config, pulsarController: PulsarController) {

  private val neighbours        = pulsarController.writerSyncProducers()
  private var processedMessages = 0

  var cancelableConsumer: Option[Consumer[Array[Byte]]] = None

  val ingestionTimer = PartitionTelemetry.totalTimeForIngestion

  var timerStart : Option[Summary.Timer] =  None

  val streamWriterVertexDeletions = PartitionTelemetry.streamWriterVertexDeletions
                                    .name("stream_writer_vertex_deletes_" + partitionID)
                                    .register()

  val streamWriterEdgeDeletions = PartitionTelemetry.streamWriterEdgeDeletions
                                  .name("stream_writer_edge_deletes_" + partitionID)
                                  .register()

  val streamWriterVertexAdditions = PartitionTelemetry.streamWriterVertexAdditions
                                    .name("total_stream_writer_vertex_adds_" + partitionID)
                                    .register()

  val streamWriterEdgeAdditions = PartitionTelemetry.streamWriterEdgeAdditions
                                  .name("total_stream_writer_edge_adds_" + partitionID)
                                  .register()

  val streamWriterGraphUpdates = PartitionTelemetry.streamWriterGraphUpdates
                                 .name("stream_writer_graph_updates_" + partitionID)
                                 .register()

  val totalSyncedStreamWriterUpdates = PartitionTelemetry.totalSyncedStreamWriterUpdates
    .name("total_synced_stream_writer_updates_" + partitionID)
    .register()

  val streamWriterRemoteGraphUpdates = PartitionTelemetry.streamWriterRemoteGraphUpdates
    .name("total_stream_writer_remote_graph_updates_" + partitionID)
    .register()


  override def run(): Unit =
    cancelableConsumer = Some(
            pulsarController
              .startPartitionConsumer(partitionID, messageListener())
    )
    timerStart = Option(ingestionTimer.startTimer())

  override def stop(): Unit = {

    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }
    neighbours.foreach(_._2.close())
    timerStart match {
      case Some(value) =>
        value.observeDuration()
      case None        =>
    }
  }

  override def handleMessage(msg: GraphAlteration): Unit = {
    msg match {
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
        streamWriterRemoteGraphUpdates.inc()

      case update: SyncExistingEdgeRemoval      =>
        processSyncExistingEdgeRemoval(
                update
        ) //A remote worker is asking for the deletion of an existing edge
        streamWriterRemoteGraphUpdates.inc()

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
        streamWriterRemoteGraphUpdates.inc()

      case update: EdgeSyncAck          =>
        processEdgeSyncAck(update) //The remote worker acknowledges the completion of an edge sync
        streamWriterRemoteGraphUpdates.inc()

      case update: VertexRemoveSyncAck  => processVertexRemoveSyncAck(update)

      case other =>
        logger.error(s"Partition '$partitionID': Received unsupported message type '$other'.")
        throw new IllegalStateException(
                s"Partition '$partitionID': Received unsupported message '$other'."
        )
    }

    printUpdateCount()
  }

  // Graph Updates from the builders
  def processVertexAdd(update: VertexAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    streamWriterVertexAdditions.inc()

    storage.addVertex(update.updateTime, update.srcId, update.properties, update.vType)
    storage.timings(update.updateTime)
  }

  def processEdgeAdd(update: EdgeAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeAdd message '$update'.")
    streamWriterEdgeAdditions.inc()

    storage.timings(update.updateTime)
    storage.addEdge(
            update.updateTime,
            update.srcId,
            update.dstId,
            update.properties,
            update.eType
    ) match {
      case Some(value) =>
        neighbours(getWriter(value.updateId)).sendAsync(serialise(value))
        storage.trackEdgeAddition(update.updateTime, update.srcId, update.dstId)
      case None        => //Edge is local
    }
  }

  def processEdgeDelete(update: EdgeDelete): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeDelete message '$update'.")
    streamWriterEdgeDeletions.inc()

    storage.timings(update.updateTime)
    storage.removeEdge(update.updateTime, update.srcId, update.dstId) match {
      case Some(value) =>
        neighbours(getWriter(value.updateId)).sendAsync(serialise(value))
        storage.trackEdgeDeletion(update.updateTime, update.srcId, update.dstId)
      case None        => //Edge is local
    }
  }

  def processVertexDelete(update: VertexDelete): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexDelete message '$update'.")
    streamWriterVertexDeletions.inc()

    val edgeRemovals = storage.removeVertex(update.updateTime, update.srcId)
    if (edgeRemovals.nonEmpty) {
      edgeRemovals.foreach(effect =>
        neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))
      )
      storage.trackVertexDeletion(update.updateTime, update.srcId, edgeRemovals.size)
    }
  }

  // Graph Effects for syncing edge adds
  def processSyncNewEdgeAdd(req: SyncNewEdgeAdd): Unit = {
    logger.trace("A writer has requested a new edge sync for a destination node in this worker.")

    storage.timings(req.msgTime)
    val effect = storage
      .syncNewEdgeAdd(req.msgTime, req.srcId, req.dstId, req.properties, req.removals, req.vType)
    neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))

    totalSyncedStreamWriterUpdates.inc()
  }

  def processSyncExistingEdgeAdd(req: SyncExistingEdgeAdd): Unit = {
    logger.trace(
            s"Partition '$partitionID': A writer has requested an existing edge sync for a destination node on in this worker."
    )

    storage.timings(req.msgTime)
    val effect = storage.syncExistingEdgeAdd(req.msgTime, req.srcId, req.dstId, req.properties)
    neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))

    totalSyncedStreamWriterUpdates.inc()
  }

  // Graph Effects for syncing edge deletions
  def processSyncNewEdgeRemoval(req: SyncNewEdgeRemoval): Unit = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for a new edge to be removed for a destination node in this worker."
    )

    storage.timings(req.msgTime)
    val effect = storage.syncNewEdgeRemoval(req.msgTime, req.srcId, req.dstId, req.removals)
    neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))

    totalSyncedStreamWriterUpdates.inc()
  }

  def processSyncExistingEdgeRemoval(req: SyncExistingEdgeRemoval): Unit = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for the deletion of an existing edge."
    )

    storage.timings(req.msgTime)
    val effect = storage.syncExistingEdgeRemoval(req.msgTime, req.srcId, req.dstId)
    neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))

    totalSyncedStreamWriterUpdates.inc()
  }

  // Graph Effects for syncing vertex deletions
  def processOutboundEdgeRemovalViaVertex(req: OutboundEdgeRemovalViaVertex): Unit = {
    logger.trace(
            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes from a vertex."
    )

    storage.timings(req.msgTime)
    val effect = storage.outboundEdgeRemovalViaVertex(req.msgTime, req.srcId, req.dstId)
    neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))
  }

  def processInboundEdgeRemovalViaVertex(req: InboundEdgeRemovalViaVertex): Unit = { //remote worker same as above
    logger.trace(
            s"Partition '$partitionID': Syncs the deletion of an edge, but for when the removal comes to a vertex."
    )

    val effect = storage.inboundEdgeRemovalViaVertex(req.msgTime, req.srcId, req.dstId)
    neighbours(getWriter(effect.updateId)).sendAsync(serialise(effect))
  }

  // Responses from the secondary server
  def processSyncExistingRemovals(req: SyncExistingRemovals): Unit = { //when the new edge add is responded to we can say it is synced
    logger.trace(
            s"Partition '$partitionID': The remote worker has returned all removals in the destination node -- for new edges"
    )

    storage.syncExistingRemovals(req.msgTime, req.srcId, req.dstId, req.removals)
    untrackEdgeUpdate(req.msgTime, req.srcId, req.dstId, req.fromAddition)

    totalSyncedStreamWriterUpdates.inc()
  }

  def processEdgeSyncAck(req: EdgeSyncAck): Unit = {
    logger.trace(
            s"Partition '$partitionID': The remote worker acknowledges the completion of an edge sync."
    )

    untrackEdgeUpdate(
            req.msgTime,
            req.srcId,
            req.dstId,
            req.fromAddition
    ) //when the edge isn't new we will get this response instead

    totalSyncedStreamWriterUpdates.inc()
  }

  private def untrackEdgeUpdate(msgTime: Long, srcId: Long, dstId: Long, fromAddition: Boolean) =
    if (fromAddition)
      storage.untrackEdgeAddition(msgTime, srcId, dstId)
    else
      storage.untrackEdgeDeletion(msgTime, srcId, dstId)

  def processVertexRemoveSyncAck(req: VertexRemoveSyncAck): Unit = {
    logger.trace(
            s"Partition '$partitionID': The remote worker acknowledges the completion of vertex removal."
    )

    storage.untrackVertexDeletion(req.msgTime, req.updateId)

    totalSyncedStreamWriterUpdates.inc()
  }

  private def dedupe(): Unit = storage.deduplicate()

  def printUpdateCount() = {
    processedMessages += 1
    streamWriterGraphUpdates.inc()

    // TODO Should this be externalised?
    //  Do we need it now that we have progress tracker?
    if (processedMessages % 100_000 == 0)
      logger.debug(
              s"Partition '$partitionID': Processed '$processedMessages' messages."
      )
  }

}
