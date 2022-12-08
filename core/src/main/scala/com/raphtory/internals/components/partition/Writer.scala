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
    graphID: String,
    partitionID: Int,
    storage: GraphPartition,
    conf: Config
)(implicit F: Async[F]) {
  private val partitioner = Partitioner(conf)

  private val vertexAdditionCount = TelemetryReporter.writerVertexAdditions.labels(partitionID.toString, graphID)
  private val edgeAddCount        = TelemetryReporter.writerEdgeAdditions.labels(partitionID.toString, graphID)

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def processUpdates(req: protocol.GraphAlterations): F[Unit] =
    Async[F].blocking(handleLocalAlterations(req))

  private def handleLocalAlterations(msgs: protocol.GraphAlterations): Unit =
    msgs.alterations.view
      .map(_.alteration)
      .foreach {
        case update: VertexAdd => processVertexAdd(update)
        case update: EdgeAdd   => processEdgeAdd(update)
      }

  // TODO: bring back `processedMessages += 1`

  def processVertexAdd(update: VertexAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received VertexAdd message '$update'.")
    storage.addVertex(update.sourceID, update.updateTime, update.index, update.srcId, update.properties, update.vType)
    vertexAdditionCount.inc()
  }

  def processEdgeAdd(upd: EdgeAdd): Unit = {
    logger.trace(s"Partition $partitionID: Received EdgeAdd message '$upd'.")
    val srcPartition = partitioner.getPartitionForId(upd.srcId)
    val dstPartition = partitioner.getPartitionForId(upd.dstId)

    if (srcPartition == partitionID && dstPartition == partitionID)
      storage.addLocalEdge(upd.sourceID, upd.updateTime, upd.index, upd.srcId, upd.dstId, upd.properties, upd.eType)
    else if (srcPartition == partitionID)
      storage.addOutgoingEdge(upd.sourceID, upd.updateTime, upd.index, upd.srcId, upd.dstId, upd.properties, upd.eType)
    else
      storage.addIncomingEdge(upd.sourceID, upd.updateTime, upd.index, upd.srcId, upd.dstId, upd.properties, upd.eType)

    edgeAddCount.inc()
  }
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
      writer <- Resource.eval(Async[F].delay(new Writer[F](graphID, partitionID, storage, conf)))
    } yield writer

  private def startupMessage[F[_]: Async](graphId: String, partitionId: Int) =
    Async[F].delay(logger.debug(s"Starting writer for graph '$graphId' and partition '$partitionId'"))
}
