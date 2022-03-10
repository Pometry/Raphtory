package com.raphtory.core.components.graphbuilder

import com.raphtory.core.components.partition.BatchWriter
import com.raphtory.core.graph._
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory
import com.typesafe.config.Config

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait GraphBuilder[T] extends Serializable {

  val logger: Logger                                         = Logger(LoggerFactory.getLogger(this.getClass))
  private var updates: ArrayBuffer[GraphUpdate]              = ArrayBuffer()
  private var partitionIDs: mutable.Set[Int]                 = _
  private var batchWriters: mutable.Map[Int, BatchWriter[T]] = _
  private var batching: Boolean                              = false
  private var totalPartitions: Int                           = 1

  def parseTuple(tuple: T): Unit

  def assignID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)

  private[core] def getUpdates(tuple: T)(failOnError: Boolean = true): List[GraphUpdate] = {
    try {
      logger.trace(s"Parsing tuple: $tuple")
      parseTuple(tuple)
    }
    catch {
      case e: Exception =>
        if (failOnError)
          throw e
        else {
          logger.warn(s"Failed to parse tuple.", e.getMessage)
          e.printStackTrace()
        }
    }

    val toReturn = updates
    updates = ArrayBuffer()

    toReturn.toList
  }

  private[core] def setupBatchIngestion(
      IDs: mutable.Set[Int],
      writers: mutable.Map[Int, BatchWriter[T]],
      partitions: Int
  ) = {
    partitionIDs = IDs
    batchWriters = writers
    batching = true
    totalPartitions = partitions
  }

  protected def addVertex(updateTime: Long, srcId: Long): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), None)
    handleVertexAdd(update)
  }

  protected def addVertex(updateTime: Long, srcId: Long, properties: Properties): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, None)
    handleVertexAdd(update)
  }

  protected def addVertex(updateTime: Long, srcId: Long, vertexType: Type): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), Some(vertexType))
    handleVertexAdd(update)
  }

  protected def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Type
  ): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, Some(vertexType))
    handleVertexAdd(update)
  }

  protected def deleteVertex(updateTime: Long, srcId: Long): Unit =
    updates += VertexDelete(updateTime, srcId)

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), None)
    handleEdgeAdd(update)
  }

  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, None)
    handleEdgeAdd(update)

  }

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, edgeType: Type): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), Some(edgeType))
    handleEdgeAdd(update)

  }

  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Type
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, Some(edgeType))
    handleEdgeAdd(update)

  }

  protected def deleteEdge(updateTime: Long, srcId: Long, dstId: Long): Unit =
    updates += EdgeDelete(updateTime, srcId, dstId)

  private def handleVertexAdd(update: VertexAdd) =
    if (batching) {
      val partitionForTuple = checkPartition(update.srcId)
      if (partitionIDs contains partitionForTuple)
        batchWriters(partitionForTuple).handleMessage(update)
    }
    else
      updates += update

  private def handleEdgeAdd(update: EdgeAdd) =
    if (batching) {
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
    }
    else
      updates += update

  private def checkPartition(id: Long): Int =
    (id.abs % totalPartitions).toInt
}

object GraphBuilder {

  def assignID(uniqueChars: String): Long =
    LongHashFunction.xx3().hashChars(uniqueChars)
}
