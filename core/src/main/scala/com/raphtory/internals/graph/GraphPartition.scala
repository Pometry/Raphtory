package com.raphtory.internals.graph

import com.raphtory.api.input.{Properties, Type}
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.graph.GraphAlteration.GraphUpdateEffect
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.{ArrowGraphLens, ArrowPartition}
import com.raphtory.internals.storage.pojograph.{PojoBasedPartition, PojoGraphLens}
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** Singleton representing the Storage for the entities
  */
abstract private[raphtory] class GraphPartition(graphID: String, partitionID: Int, conf: Config) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val watermarker = new Watermarker(graphID, this)

  // Ingesting Vertices
  def addVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit

  def removeVertex(sourceID: Long, msgTime: Long, index: Long, srcId: Long): List[GraphUpdateEffect]

  def inboundEdgeRemovalViaVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphUpdateEffect

  def outboundEdgeRemovalViaVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphUpdateEffect

  // Ingesting Edges
  def addEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphUpdateEffect]

  def syncNewEdgeAdd(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      srcRemovals: List[(Long, Long)],
      edgeType: Option[Type]
  ): GraphUpdateEffect

  def syncExistingEdgeAdd(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): GraphUpdateEffect

  def removeEdge(sourceID: Long, msgTime: Long, index: Long, srcId: Long, dstId: Long): Option[GraphUpdateEffect]

  def syncNewEdgeRemoval(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      srcRemovals: List[(Long, Long)]
  ): GraphUpdateEffect
  def syncExistingEdgeRemoval(sourceID: Long, msgTime: Long, index: Long, srcId: Long, dstId: Long): GraphUpdateEffect

  def syncExistingRemovals(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      dstRemovals: List[(Long, Long)]
  ): Unit

  // Analysis Functions
  def getVertices(
      graphPerspective: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex]

  // Partition Neighbours
  private val totalPartitions = conf.getInt("raphtory.partitions.countPerServer") *
    conf.getInt("raphtory.partitions.serverCount")

  def getPartitionID: Int = partitionID

  def checkDst(dstID: Long): Boolean =
    GraphPartition.checkDst(dstID, totalPartitions, partitionID)

  def timings(updateTime: Long): Unit = {
    if (updateTime < watermarker.oldestTime.get() && updateTime > 0)
      watermarker.oldestTime.set(updateTime)
    if (updateTime > watermarker.latestTime.get())
      watermarker.latestTime.set(updateTime)
  }

  def lens(
      jobID: String,
      actualStart: Long,
      actualEnd: Long,
      superStep: Int,
      sendMessage: GenericVertexMessage[_] => Unit,
      errorHandler: Throwable => Unit,
      scheduler: Scheduler
  ): LensInterface =
    this match {
      case partition: ArrowPartition     =>
        ArrowGraphLens(
                jobID,
                actualStart,
                actualEnd,
                superStep,
                partition,
                sendMessage,
                errorHandler,
                scheduler
        )
      case partition: PojoBasedPartition =>
        PojoGraphLens(
                jobID,
                actualStart,
                actualEnd,
                superStep = 0,
                partition,
                conf,
                sendMessage,
                errorHandler,
                scheduler
        )
    }
}

object GraphPartition {

  def checkDst(dstID: Long, totalPartitions: Int, partitionID: Int): Boolean =
    (dstID.abs % totalPartitions).toInt == partitionID

}
