package com.raphtory.internals.storage.pojograph

import com.raphtory.api.input._
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internals.storage.pojograph.entities.internal.PojoVertex
import com.typesafe.config.Config
import scala.collection.mutable
import DisruptorQueue._

private[raphtory] class PojoBasedPartition(graphID: String, partitionID: Int, conf: Config)
        extends GraphPartition(graphID, partitionID, conf) {
  private val hasDeletionsPath      = "raphtory.data.containsDeletions"
  private val hasDeletions: Boolean = conf.getBoolean(hasDeletionsPath)

  logger.debug(
          s"Config indicates that the data contains 'delete' events. " +
            s"To change this modify '$hasDeletionsPath' in the application conf."
  )

  private val q = DisruptorQueue(graphID, partitionID)

  // If the add come with some properties add all passed properties into the entity
  override def addVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit =
    q.addAddVertexReqToQueue(msgTime, index, srcId, properties, vertexType)

  override def addVertex(vAdd: VertexAdd): Unit = {
    val VertexAdd(sourceID, updateTime, index, srcId, properties, vType) = vAdd
    addVertex(sourceID, updateTime, index, srcId, properties, vType)
  }

  override def addLocalEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    // Create or revive the src vertex
    q.addAddVertexReqToQueue(msgTime, index, srcId, Properties(), None)
    logger.trace(s"Src ID: $srcId created and revived")

    // Create or revive the dst vertex
    if (srcId != dstId) {
      q.addAddVertexReqToQueue(msgTime, index, dstId, Properties(), None)

      val edge = new PojoEdge(msgTime, index, srcId, dstId, initialValue = true)
      q.addAddEdgeReqToQueue(msgTime, index, srcId, srcId, dstId, properties, edgeType, LocalOutgoingEdge(edge))
      q.addAddEdgeReqToQueue(msgTime, index, dstId, srcId, dstId, properties, edgeType, LocalIncomingEdge(edge))
    }
    else
      q.addAddEdgeReqToQueue(msgTime, index, srcId, srcId, dstId, properties, edgeType, LocalEdge)
  }

  override def addLocalEdge(eAdd: EdgeAdd): Unit = {
    val EdgeAdd(sourceID, updateTime, index, srcId, dstId, properties, eType) = eAdd
    addLocalEdge(sourceID, updateTime, index, srcId, dstId, properties, eType)
  }

  override def addOutgoingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    // Create or revive the src vertex
    q.addAddVertexReqToQueue(msgTime, index, srcId, Properties(), None)
    logger.trace(s"Src ID: $srcId created and revived")

    q.addAddEdgeReqToQueue(msgTime, index, srcId, srcId, dstId, properties, edgeType, RemoteOutgoingEdge)
  }

  override def addIncomingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    // Create or revive the src vertex
    q.addAddVertexReqToQueue(msgTime, index, dstId, Properties(), None)
    logger.trace(s"Dst ID: $srcId created and revived")

    q.addAddEdgeReqToQueue(msgTime, index, dstId, srcId, dstId, properties, edgeType, RemoteIncomingEdge)
  }

  override def getVertices(
      lens: LensInterface,
      start: Long,
      end: Long
  ): mutable.Map[Long, PojoExVertex] =
    q.getVertices(
            start,
            end,
            (id: Long, vertex: PojoVertex) => (id, vertex.viewBetween(start, end, lens.asInstanceOf[PojoGraphLens]))
    )

  override def flush: Unit = q.flush()
}
