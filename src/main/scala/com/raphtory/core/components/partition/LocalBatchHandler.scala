package com.raphtory.core.components.partition

import com.raphtory.core.components.Component
import com.raphtory.core.components.graphbuilder._
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config

import scala.collection.mutable
import scala.reflect.ClassTag

class LocalBatchHandler[T: ClassTag](
    partitionIDs: mutable.Set[Int],
    batchWriters: mutable.Map[Int, BatchWriter[T]],
    spout: Spout[T],
    graphBuilder: GraphBuilder[T],
    conf: Config,
    pulsarController: PulsarController
) extends Component[GraphAlteration](conf: Config, pulsarController: PulsarController) {

  override def handleMessage(
      msg: GraphAlteration
  ): Unit = {} //No messages received by this component

  override def run(): Unit =
    while (spout.hasNext())
      spout.next() match {
        case Some(tuple) =>
          graphBuilder.getUpdates(tuple)(failOnError = false).foreach {
            case update: VertexAdd    =>
              val partitionForTuple = checkPartition(update.srcId)
              if (partitionIDs contains partitionForTuple)
                batchWriters(partitionForTuple).handleMessage(update)
            case update: EdgeAdd      =>
              val partitionForSrc = checkPartition(update.srcId)
              val partitionForDst = checkPartition(update.dstId)
              if (partitionIDs contains partitionForSrc)
                batchWriters(partitionForSrc).handleMessage(update)
              if (
                      (partitionIDs contains partitionForDst) && (partitionForDst != partitionForSrc)
              ) //TODO doesn't see to currently work
                batchWriters(partitionForDst).handleMessage(
                        BatchAddRemoteEdge(
                                update.updateTime,
                                update.srcId,
                                update.dstId,
                                update.properties,
                                update.eType
                        )
                )
            case update: EdgeDelete   => //Not currently supported by batch ingestion
            case update: VertexDelete => //Not currently supported by batch ingestion

          }
        case None        =>
      }

  private def checkPartition(id: Long): Int =
    (id.abs % totalPartitions).toInt

  override def stop(): Unit = {}
}
