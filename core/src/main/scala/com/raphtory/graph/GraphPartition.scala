package com.raphtory.graph

import com.raphtory.components.graphbuilder.GraphUpdateEffect
import com.raphtory.components.graphbuilder.Properties._
import com.raphtory.graph.visitor.Vertex
import com.raphtory.time.Interval
import com.raphtory.storage.pojograph.entities.external.PojoExVertex
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** Singleton representing the Storage for the entities
  * @note DoNotDocument
  */
abstract class GraphPartition(partitionID: Int, conf: Config) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  protected val failOnError: Boolean = conf.getBoolean("raphtory.partitions.failOnError")
  private var batchIngesting         = false
  def startBatchIngesting()          = batchIngesting = true
  def stopBatchIngesting()           = batchIngesting = false
  def currentyBatchIngesting()       = batchIngesting

  // Ingesting Vertices
  def addVertex(msgTime: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): Unit

  def removeVertex(msgTime: Long, srcId: Long): List[GraphUpdateEffect]
  def inboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long): GraphUpdateEffect
  def outboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long): GraphUpdateEffect

  // Ingesting Edges
  def addEdge(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphUpdateEffect]

  def syncNewEdgeAdd(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      srcRemovals: List[Long],
      edgeType: Option[Type]
  ): GraphUpdateEffect

  def syncExistingEdgeAdd(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): GraphUpdateEffect

  def batchAddRemoteEdge(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit

  def removeEdge(msgTime: Long, srcId: Long, dstId: Long): Option[GraphUpdateEffect]

  def syncNewEdgeRemoval(
      msgTime: Long,
      srcId: Long,
      dstId: Long,
      srcRemovals: List[Long]
  ): GraphUpdateEffect
  def syncExistingEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long): GraphUpdateEffect

  def syncExistingRemovals(msgTime: Long, srcId: Long, dstId: Long, dstRemovals: List[Long]): Unit

  def deduplicate()

  // Analysis Functions
  def getVertices(
      graphPerspective: GraphLens,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex]

  // Partition Neighbours
  val totalPartitions                = conf.getInt("raphtory.partitions.countPerServer") * conf.getInt(
          "raphtory.partitions.serverCount"
  )
  def getPartitionID                 = partitionID
  def checkDst(dstID: Long): Boolean = (dstID.abs % totalPartitions).toInt == partitionID

  // Watermarking
  var oldestTime: Long = Long.MaxValue
  var newestTime: Long = 0

  def timings(updateTime: Long) = {
    if (updateTime < oldestTime && updateTime > 0) oldestTime = updateTime
    if (updateTime > newestTime)
      newestTime = updateTime
  }

  val blockingEdgeAdditions: mutable.TreeSet[(Long, (Long, Long))]          = mutable.TreeSet()
  val blockingEdgeDeletions: mutable.TreeSet[(Long, (Long, Long))]          = mutable.TreeSet()
  val blockingVertexDeletions: mutable.TreeMap[(Long, Long), AtomicInteger] = mutable.TreeMap()

  def trackEdgeAddition(timestamp: Long, src: Long, dst: Long): Unit =
    blockingEdgeAdditions += ((timestamp, (src, dst)))

  def trackEdgeDeletion(timestamp: Long, src: Long, dst: Long): Unit =
    blockingEdgeDeletions += ((timestamp, (src, dst)))

  def trackVertexDeletion(timestamp: Long, src: Long, size: Int): Unit =
    blockingVertexDeletions put ((timestamp, src), new AtomicInteger(size))

  def untrackEdgeAddition(timestamp: Long, src: Long, dst: Long): Unit =
    blockingEdgeAdditions -= ((timestamp, (src, dst)))

  def untrackEdgeDeletion(timestamp: Long, src: Long, dst: Long): Unit =
    blockingEdgeDeletions -= ((timestamp, (src, dst)))

  def untrackVertexDeletion(timestamp: Long, src: Long): Unit =
    blockingVertexDeletions get (timestamp, src) match {
      case Some(counter) =>
        if (
                counter.decrementAndGet() == 0
        ) //if after we remove this value its now zero we can remove from the tree
          blockingVertexDeletions -= ((timestamp, src))
      case None          => //??? // this should never happen
    }

}
