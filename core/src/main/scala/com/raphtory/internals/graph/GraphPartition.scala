package com.raphtory.internals.graph

import com.raphtory.internals.graph.GraphAlteration.GraphUpdateEffect
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.graph.GraphAlteration.GraphUpdateEffect
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config

import scala.collection.mutable

/** Singleton representing the Storage for the entities
  */
abstract private[raphtory] class GraphPartition(graphID: String, partitionID: Int, conf: Config) {

  protected val failOnError: Boolean = conf.getBoolean("raphtory.partitions.failOnError")
  private var batchIngesting         = false
  val watermarker                    = new Watermarker(graphID, this)
  def startBatchIngesting()          = batchIngesting = true
  def stopBatchIngesting()           = batchIngesting = false
  def currentyBatchIngesting()       = batchIngesting

  // Ingesting Vertices
  def addVertex(msgTime: Long, index: Long, srcId: Long, properties: Properties, vertexType: Option[Type]): Unit

  def removeVertex(msgTime: Long, index: Long, srcId: Long): List[GraphUpdateEffect]
  def inboundEdgeRemovalViaVertex(msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect
  def outboundEdgeRemovalViaVertex(msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect

  // Ingesting Edges
  def addEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphUpdateEffect]

  def syncNewEdgeAdd(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      srcRemovals: List[(Long, Long)],
      edgeType: Option[Type]
  ): GraphUpdateEffect

  def syncExistingEdgeAdd(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): GraphUpdateEffect

  def batchAddRemoteEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit

  def removeEdge(msgTime: Long, index: Long, srcId: Long, dstId: Long): Option[GraphUpdateEffect]

  def syncNewEdgeRemoval(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      srcRemovals: List[(Long, Long)]
  ): GraphUpdateEffect
  def syncExistingEdgeRemoval(msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect

  def syncExistingRemovals(msgTime: Long, index: Long, srcId: Long, dstId: Long, dstRemovals: List[(Long, Long)]): Unit

  // Analysis Functions
  def getVertices(
      graphPerspective: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex]

  // Partition Neighbours
  private val totalPartitions = conf.getInt("raphtory.partitions.countPerServer") *
    conf.getInt("raphtory.partitions.serverCount")

  def getPartitionID: Int            = partitionID
  def checkDst(dstID: Long): Boolean = (dstID.abs % totalPartitions).toInt == partitionID

  def timings(updateTime: Long): Unit = {
    if (updateTime < watermarker.oldestTime.get() && updateTime > 0)
      watermarker.oldestTime.set(updateTime)
    if (updateTime > watermarker.latestTime.get())
      watermarker.latestTime.set(updateTime)
  }

}
