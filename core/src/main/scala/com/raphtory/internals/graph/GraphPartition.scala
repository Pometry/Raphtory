package com.raphtory.internals.graph

import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.ArrowGraphLens
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** Singleton representing the Storage for the entities
  */
abstract private[raphtory] class GraphPartition(graphID: String, partitionID: Int, conf: Config) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  // Ingesting Vertices
  def addVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit

  // Ingesting Edges
  // This method should assume that both vertices are local and create them if they don't exist
  def addLocalEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit

  // This method should assume that the dstId belongs to another partition
  def addOutgoingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit

  // This method should assume that the srcId belongs to another partition
  def addIncomingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit

  // Analysis Functions
  def getVertices(
      graphPerspective: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex]

  // Partition Neighbours
  private val partitioner = Partitioner(conf)

  def getPartitionID: Int = partitionID

  def isLocal(id: Long): Boolean = partitioner.getPartitionForId(id) == partitionID

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
