package com.raphtory.internals.storage.arrow

import com.raphtory.api.input.ImmutableProperty
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
import com.raphtory.internals.graph.GraphAlteration.SyncNewEdgeAdd
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config

import java.lang
import scala.collection.AbstractView
import scala.collection.mutable

class ArrowPartition(val par: RaphtoryArrowPartition, graphID: String, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf) {

  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  private val idsRepo = new LocalEntityRepo(par.getLocalEntityIdStore, totalPartitions, partition)

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
    addVertexInternal(srcId, msgTime, properties)

  private def addVertexInternal(srcId: Long, msgTime: Long, properties: Properties): Vertex = {
    val vertex = idsRepo.resolve(srcId) match {
      case NotFound(id)          => // it's not present on this partition .. yet
        val srcLocalId = vmgr.getNextFreeVertexId
        val v          = par.getVertex
        v.reset(srcLocalId, id, true, msgTime)

        properties.properties.foreach {
          case ImmutableProperty(key, value) =>
            val FIELD = par.getVertexFieldId(key)
            v.getField(FIELD).set(new lang.StringBuilder(value))
          //      case LongProperty(key, value)   =>
          //        val FIELD = par.getVertexFieldId(key)
          //        v.getField(FIELD).set(value)
          case _                             =>
        }

        vmgr.addVertex(v)
        vmgr.addHistory(v.getLocalId, msgTime, true, -1, false)
        v
      case ExistsOnPartition(id) => // we've seen you before but are there any differences?
        val v = vmgr.getVertex(id)

        // TODO: need a way to merge properties into existing vertex
        // TODO: we can't just overwrite a vertex that already exists
        // we make sure the vertex is active at this point
        vmgr.addHistory(v.getLocalId, msgTime, true, -1, false)
        v
      case _                     => throw new IllegalStateException(s"Node $srcId does not belong to partition $getPartitionID")
    }

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

    // init the edge
    val eId = emgr.getNextFreeEdgeId
    val e   = par.getEdge
    e.init(eId, true, msgTime)

    // add the edge for the src
    // add or activate
    val src = addVertexInternal(srcId, msgTime, Properties())
    val p   = vmgr.getPartition(vmgr.getPartitionId(src.getLocalId))

    val prevListPtr = p.synchronized {
      val ptr = p.addOutgoingEdgeToList(src.getLocalId, e.getLocalId)
      p.addHistory(src.getLocalId, msgTime, true, e.getLocalId, true)
      ptr
    }

    emgr.setOutgoingEdgePtr(e.getLocalId, prevListPtr)

    // handle dst
    val dst = idsRepo.resolve(dstId)
    val out = if (dst.isLocal && checkDst(dst.id)) { // do we belong here?
      // add dst vertex if local
      addVertexInternal(dstId, msgTime, Properties())
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
      // otherwise sync new edge
      Some(SyncNewEdgeAdd(sourceID, msgTime, index, srcId, dstId, properties, Nil, edgeType))

    // finally add the edge
    e.resetEdgeData(src.getLocalId, dst.id, -1L, -1L, false, dst.isGlobal)
    emgr.addEdge(e, -1L, -1L)
    emgr.addHistory(e.getLocalId, msgTime, true)

    out
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
