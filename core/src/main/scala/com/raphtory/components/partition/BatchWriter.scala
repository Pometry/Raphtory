package com.raphtory.components.partition

import com.raphtory.components.Component
import com.raphtory.components.graphbuilder.BatchAddRemoteEdge
import com.raphtory.components.graphbuilder.EdgeAdd
import com.raphtory.components.graphbuilder.EdgeDelete
import com.raphtory.components.graphbuilder.GraphAlteration
import com.raphtory.config.telemetry.PartitionTelemetry
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.graphbuilder.SyncExistingEdgeAdd
import com.raphtory.components.graphbuilder.SyncExistingEdgeRemoval
import com.raphtory.components.graphbuilder.SyncNewEdgeAdd
import com.raphtory.components.graphbuilder.SyncNewEdgeRemoval
import com.raphtory.components.graphbuilder.VertexAdd
import com.raphtory.components.spout.Spout
import com.raphtory.config.PulsarController
import com.raphtory.graph._
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.reflect.ClassTag

/** @DoNotDocument */
class BatchWriter[T: ClassTag](
    partitionID: Int,
    storage: GraphPartition
) {

  def getStorage() = storage

  private var processedMessages = 0
  val logger: Logger            = Logger(LoggerFactory.getLogger(this.getClass))

  val batchWriterVertexAdditions = PartitionTelemetry.batchWriterVertexAdditions(partitionID)
  val batchWriterEdgeAdditions = PartitionTelemetry.batchWriterEdgeAdditions(partitionID)
  val batchWriterRemoteEdgeAdditions = PartitionTelemetry.batchWriterRemoteEdgeAdditions(partitionID)

  def handleMessage(msg: GraphAlteration): Unit = {
    msg match {
      //Updates from the Graph Builder
      //TODO Make Vertex Deletions batch ingestable
      case update: VertexAdd          => processVertexAdd(update)
      case update: EdgeAdd            => processEdgeAdd(update)
      case update: BatchAddRemoteEdge => processRemoteEdgeAdd(update)
      case update: EdgeDelete         => processEdgeDelete(update)

      case other =>
        logger.error(s"Partition '$partitionID': Received unsupported message type '$other'.")
        throw new IllegalStateException(
                s"Partition '$partitionID': Received unsupported message '$other'."
        )
    }

    printUpdateCount()
  }

  /**
    * Graph Updates from the builders
    */
  def processVertexAdd(update: VertexAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    storage.addVertex(update.updateTime, update.srcId, update.properties, update.vType)
    storage.timings(update.updateTime)
    batchWriterVertexAdditions.inc()
  }

  def processEdgeAdd(update: EdgeAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeAdd message '$update'.")
    storage.timings(update.updateTime)
    storage.addEdge(
            update.updateTime,
            update.srcId,
            update.dstId,
            update.properties,
            update.eType
    )
    batchWriterEdgeAdditions.inc()
  }

  def processRemoteEdgeAdd(req: BatchAddRemoteEdge): Unit = {
    logger.trace("A writer has requested a new edge sync for a destination node in this worker.")

    storage.timings(req.msgTime)
    storage
      .batchAddRemoteEdge(req.msgTime, req.srcId, req.dstId, req.properties, req.vType)
    batchWriterRemoteEdgeAdditions.inc()
  }

  def processSyncNewEdgeRemoval(req: SyncNewEdgeRemoval): Unit = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for a new edge to be removed for a destination node in this worker."
    )
    storage.timings(req.msgTime)
    storage.syncNewEdgeRemoval(req.msgTime, req.srcId, req.dstId, req.removals)
    batchWriterRemoteEdgeAdditions.inc()
  }

  def processEdgeDelete(update: EdgeDelete): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeDelete message '$update'.")
    storage.timings(update.updateTime)
    storage.removeEdge(update.updateTime, update.srcId, update.dstId)
  }

  def printUpdateCount() = {
    processedMessages += 1

    // TODO Should this be externalised?
    //  Do we need it now that we have progress tracker?
    if (processedMessages % 100_000 == 0)
      logger.debug(
              s"Partition '$partitionID': Processed '$processedMessages' messages."
      )
  }

}
