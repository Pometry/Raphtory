package com.raphtory.components.partition

import com.raphtory.components.graphbuilder._
import com.raphtory.config.telemetry.ComponentTelemetryHandler
import com.raphtory.graph._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.reflect.ClassTag

/** @note DoNotDocument */
class BatchWriter[T: ClassTag](
    partitionID: Int,
    storage: GraphPartition
) {
  private val telemetry: ComponentTelemetryHandler.type = ComponentTelemetryHandler

  def getStorage() = storage

  private var processedMessages = 0
  private val logger: Logger    = Logger(LoggerFactory.getLogger(this.getClass))

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

  def processVertexAdd(update: VertexAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    storage.addVertex(update.updateTime, update.srcId, update.properties, update.vType)
    storage.timings(update.updateTime)
    telemetry.batchWriterVertexAdditionsCollector.labels(partitionID.toString).inc()
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
    telemetry.batchWriterEdgeAdditionsCollector.labels(partitionID.toString).inc()
  }

  def processRemoteEdgeAdd(req: BatchAddRemoteEdge): Unit = {
    logger.trace("A writer has requested a new edge sync for a destination node in this worker.")

    storage.timings(req.msgTime)
    storage
      .batchAddRemoteEdge(req.msgTime, req.srcId, req.dstId, req.properties, req.vType)
    telemetry.batchWriterRemoteEdgeAdditionsCollector.labels(partitionID.toString).inc()
  }

  def processSyncNewEdgeRemoval(req: SyncNewEdgeRemoval): Unit = {
    logger.trace(
            s"Partition '$partitionID': A remote worker is asking for a new edge to be removed for a destination node in this worker."
    )
    storage.timings(req.msgTime)
    storage.syncNewEdgeRemoval(req.msgTime, req.srcId, req.dstId, req.removals)
    telemetry.batchWriterRemoteEdgeDeletionsCollector.labels(partitionID.toString).inc()
  }

  def processEdgeDelete(update: EdgeDelete): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeDelete message '$update'.")
    storage.timings(update.updateTime)
    storage.removeEdge(update.updateTime, update.srcId, update.dstId)
    telemetry.batchWriterEdgeDeletionsCollector.labels(partitionID.toString).inc()
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
