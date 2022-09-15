package com.raphtory.internals.storage.arrow

import com.raphtory.api.input.LongProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.StringProperty
import com.raphtory.api.input.Type
import com.raphtory.arrowcore.implementation.EdgeIterator.AllEdgesIterator
import com.raphtory.arrowcore.implementation.VertexIterator.AllVerticesIterator
import com.raphtory.arrowcore.implementation.EdgePartitionManager
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.implementation.{VertexIterator => ArrVertexIter}
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config

import java.lang
import scala.collection.AbstractView
import scala.collection.mutable

class ArrowPartition(val par: RaphtoryArrowPartition, graphID: String, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf) {

  private val idsRepo = new LocalEntityRepo(par.getLocalEntityIdStore)

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
  ): Unit =
    addVertexInternal(msgTime, srcId, properties)

  private def addVertexInternal(srcId: Long, msgTime: Long, properties: Properties): Vertex = {
    val vertex = idsRepo.resolve(srcId) match {
      case GlobalId(id) => // it's not present on this partition .. yet
        val srcLocalId = vmgr.getNextFreeVertexId
        val v          = par.getVertex
        v.reset(srcLocalId, id, true, msgTime)

        properties.properties.foreach {
          case StringProperty(key, value) =>
            val FIELD = par.getVertexFieldId(key)
            v.getField(FIELD).set(new lang.StringBuilder(value))
          //      case LongProperty(key, value)   =>
          //        val FIELD = par.getVertexFieldId(key)
          //        v.getField(FIELD).set(value)
          case _                          =>
        }

        v
      case LocalId(id)  => // we've seen you before
        val v = vmgr.getVertex(id)
        v
    }

    // fixme: things are strange if we add a vertex without any changes
    vmgr.addVertex(vertex)
    vmgr.addHistory(vertex.getLocalId, msgTime, true, -1, false)
    vertex
  }

  override def addEdge(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphAlteration.GraphUpdateEffect] = {

    val dst = idsRepo.resolve(dstId)

    // add or activate
    val srcVertex = addVertexInternal(srcId, msgTime, Properties())

    val eId = emgr.getNextFreeEdgeId
    val e   = par.getEdge
    e.init(eId, true, msgTime)

    if (dst.isLocal) {
      val p           = vmgr.getPartition(vmgr.getPartitionId(dst.id))
      val prevListPtr = p.synchronized {
        val ptr = p.addIncomingEdgeToList(dst.id, e.getLocalId)
        p.addHistory(dst.id, msgTime, true, e.getLocalId, false)
        ptr
      }

      emgr.setIncomingEdgePtr(e.getLocalId, prevListPtr)
      None
    }
    else
      // if the edge exists then we send a SyncExistingEdge
      // otherwise sync new edge
      ???

//    if (src.isLocal) {
//      val p           = vmgr.getPartition(vmgr.getPartitionId(src.id))
//      val prevListPtr = p.synchronized {
//        val ptr = p.addOutgoingEdgeToList(src.id, e.getLocalId)
//        p.addHistory(src.id, msgTime, true, e.getLocalId, true)
//        ptr
//      }
//
//      emgr.setOutgoingEdgePtr(e.getLocalId, prevListPtr)
//    }

  }

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
}

object ArrowPartition {

  class VertexIterator(vs: AllVerticesIterator) extends Iterator[Vertex] {
    override def hasNext: Boolean = vs.hasNext

    override def next(): Vertex = vs.getVertex
  }

  class EdgesIterator(es: AllEdgesIterator) extends Iterator[Edge] {
    override def hasNext: Boolean = es.hasNext

    override def next(): Edge = es.getEdge
  }

  def apply(cfg: ArrowPartitionConfig, config: Config): ArrowPartition = {
    val graphID     = config.getString("raphtory.graph.id")
    val arrowConfig = new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig)

    new ArrowPartition(arrowConfig, graphID, arrowConfig.getRaphtoryPartitionId, config)
  }
}
