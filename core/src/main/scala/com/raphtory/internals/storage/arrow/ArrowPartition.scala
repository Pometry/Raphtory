package com.raphtory.internals.storage.arrow

import com.raphtory.api.input.LongProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.StringProperty
import com.raphtory.api.input.Type
import com.raphtory.arrowcore.implementation.VertexIterator.AllVerticesIterator
import com.raphtory.arrowcore.implementation.EdgePartitionManager
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.implementation.{VertexIterator => ArrVertexIter}
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config

import scala.collection.AbstractView
import scala.collection.mutable

class ArrowPartition(val par: RaphtoryArrowPartition, graphID: String, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf) {

  def vertices: AbstractView[Vertex] =
    new AbstractView[Vertex] {
      override def iterator: Iterator[Vertex] = new ArrowPartition.VertexIterator(par.getNewAllVerticesIterator)

      // can we have more than 2 billion vertices per partition?
      override def knownSize: Int = par.getVertexMgr.getTotalNumberOfVertices.toInt
    }

//  def windowVertices(start: Long, end: Long): AbstractView[Vertex] =
//    new AbstractView[Vertex] {
//
//      override def iterator: Iterator[Vertex] =
//        new ArrowPartition.VertexIterator(par.getNewWindowedVertexIterator(start, end))
//    }

  private def vmgr: VertexPartitionManager = par.getVertexMgr
  private def emgr: EdgePartitionManager   = par.getEdgeMgr

  override def addVertex(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit = {

    val srcLocalId = vmgr.getNextFreeVertexId
    val v          = par.getVertex
    v.reset(srcLocalId, srcId, true, msgTime)

    properties.properties.foreach {
      case StringProperty(key, value) =>
        val FIELD = par.getVertexFieldId(key)
        v.getField(FIELD).set(new java.lang.StringBuilder(value))
//      case LongProperty(key, value)   =>
//        val FIELD = par.getVertexFieldId(key)
//        v.getField(FIELD).set(value)
      case _ =>
    }

    vmgr.addVertex(v)

  }

  override def removeVertex(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long
  ): List[GraphAlteration.GraphUpdateEffect] = ???

  override def inboundEdgeRemovalViaVertex(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphAlteration.GraphUpdateEffect = ???

  override def outboundEdgeRemovalViaVertex(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphAlteration.GraphUpdateEffect = ???

  override def addEdge(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphAlteration.GraphUpdateEffect] = ???

  override def syncNewEdgeAdd(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      srcRemovals: List[(Long, Long)],
      edgeType: Option[Type]
  ): GraphAlteration.GraphUpdateEffect = ???

  override def syncExistingEdgeAdd(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): GraphAlteration.GraphUpdateEffect = ???

  override def batchAddRemoteEdge(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = ???

  override def removeEdge(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): Option[GraphAlteration.GraphUpdateEffect] = ???

  override def syncNewEdgeRemoval(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      srcRemovals: List[(Long, Long)]
  ): GraphAlteration.GraphUpdateEffect = ???

  override def syncExistingEdgeRemoval(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphAlteration.GraphUpdateEffect = ???

  override def syncExistingRemovals(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      dstRemovals: List[(Long, Long)]
  ): Unit = ???

  override def getVertices(graphPerspective: LensInterface, start: Long, end: Long): mutable.Map[Long, PojoExVertex] =
    ???
}

object ArrowPartition {

  class VertexIterator(vs: AllVerticesIterator) extends Iterator[Vertex] {
    override def hasNext: Boolean = vs.hasNext

    override def next(): Vertex = vs.getVertex
  }

  def apply(cfg: ArrowPartitionConfig, config: Config): ArrowPartition = {
    val graphID     = config.getString("raphtory.graph.id")
    val arrowConfig = new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig)

    new ArrowPartition(arrowConfig, graphID, arrowConfig.getRaphtoryPartitionId, config)
  }
}
