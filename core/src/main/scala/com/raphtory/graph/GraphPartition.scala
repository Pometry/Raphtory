package com.raphtory.graph

import com.raphtory.components.graphbuilder.GraphUpdateEffect
import com.raphtory.components.graphbuilder.Properties._
import com.raphtory.components.querymanager.WatermarkTime
import com.raphtory.storage.pojograph.entities.external.PojoExVertex
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.atomic.AtomicLong
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.Map
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/** Singleton representing the Storage for the entities
  * @note DoNotDocument
  */
abstract class GraphPartition(partitionID: Int, conf: Config) {

  protected val failOnError: Boolean = conf.getBoolean("raphtory.partitions.failOnError")
  private var batchIngesting         = false
  val watermarker                    = new Watermarker(this)
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

  // Analysis Functions
  def getVertices(
      graphPerspective: GraphLens,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex]

  // Partition Neighbours
  private val totalPartitions = conf.getInt("raphtory.partitions.countPerServer") *
    conf.getInt("raphtory.partitions.serverCount")

  def getPartitionID                 = partitionID
  def checkDst(dstID: Long): Boolean = (dstID.abs % totalPartitions).toInt == partitionID

  def timings(updateTime: Long) = {
    if (updateTime < watermarker.oldestTime.get() && updateTime > 0)
      watermarker.oldestTime.set(updateTime)
    if (updateTime > watermarker.latestTime.get())
      watermarker.latestTime.set(updateTime)
  }

}
