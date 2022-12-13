package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.input._
import com.raphtory.arrowcore.implementation.ArrowPropertyIterator
import com.raphtory.arrowcore.implementation.EdgeHistoryIterator
import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EdgePartitionManager
import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.LocalEntityIdStore
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexHistoryIterator
import com.raphtory.arrowcore.implementation.VertexPartition
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config

import java.lang
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.atomic.LongAccumulator
import scala.collection.AbstractView
import scala.collection.View
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ArrowPartition(graphID: String, val par: RaphtoryArrowPartition, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf)
        with AutoCloseable {

  def asGlobal(vertexId: Long): Long =
    par.getVertexMgr.getVertex(vertexId).getGlobalId

  def getVertex(id: Long): Vertex =
    par.getVertexMgr.getVertex(id)

  def vertexCount: Int = par.getVertexMgr.getTotalNumberOfVertices.toInt

  val min: LongAccumulator = new LongAccumulator(Math.min(_, _), Long.MaxValue)
  val max: LongAccumulator = new LongAccumulator(Math.max(_, _), Long.MinValue)

  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  def localEntityStore: LocalEntityIdStore = par.getLocalEntityIdStore

  def vertices: View[Vertex] =
    new AbstractView[Vertex] {
      override def iterator: Iterator[Vertex] = new ArrowPartition.VertexIterator(par.getNewAllVerticesIterator)

      // can we have more than 2 billion vertices per partition?
      override def knownSize: Int = par.getVertexMgr.getTotalNumberOfVertices.toInt
    }

  def windowVertices(start: Long, end: Long): View[Vertex] =
    if (start <= min.longValue() && end >= max.longValue())
      vertices // don't bother filtering if the interval is greater than min and max
    else
      new AbstractView[Vertex] {

        override def iterator: Iterator[Vertex] =
          new ArrowPartition.VertexIterator(par.getNewWindowedVertexIterator(start, end))
      }

  private def vmgr: VertexPartitionManager = par.getVertexMgr
  private def emgr: EdgePartitionManager   = par.getEdgeMgr

  override def addVertex(
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit =
    addVertexInternal(srcId, msgTime, properties)

  @inline
  private def updateAdders(msgTime: Long): Unit = {
//    min.accumulate(msgTime)
//    max.accumulate(msgTime)
  }

  private def addVertexInternal(srcId: Long, msgTime: Long, properties: Properties): Vertex =
    /*this.synchronized */{

      updateAdders(msgTime)

      val localSrcId = localEntityStore.getLocalNodeId(srcId)

      if (localSrcId == -1) {
        val srcLocalId = vmgr.getNextFreeVertexId
        createVertex(srcLocalId, srcId, msgTime, properties)
      }
      else {
        val v = vmgr.getVertex(localSrcId)

//        val props: Set[String] =
//          par.getPropertySchema.versionedVertexProperties().asScala.map(_.name()).toSet intersect properties.properties
//            .map(_.key.toLowerCase())
//            .toSet

        addOrUpdateVertexProperties(
                msgTime,
                v,
          properties
        )
        vmgr.addHistory(v.getLocalId, msgTime, true, properties.properties.nonEmpty, -1, false)
        v
      }

    }

  private def partitionFromVertex(v: Vertex) =
    par.getVertexMgr.getPartition(par.getVertexMgr.getPartitionId(v.getLocalId))

  private def partitionFromVertex2(vertexLocalId: Long) =
    par.getVertexMgr.getPartition(par.getVertexMgr.getPartitionId(vertexLocalId))

  private def setProps(e: Entity, msgTime: Long, properties: Properties)(
      lookupProp: String => Int
  )(lookupField: String => Int): Unit =
    properties.properties.foreach {
      case ImmutableString(key, value) =>
        val FIELD    = lookupField(key.toLowerCase())
        val accessor = e.getField(FIELD)
        accessor.set(new lang.StringBuilder(value))
      case MutableString(key, value)   =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(new lang.StringBuilder(value))
      case MutableLong(key, value)     =>
        val FIELD = lookupProp(key)
        if (!e.getProperty(FIELD).isSet)
          e.getProperty(FIELD).setHistory(true, msgTime).set(value)
        else {
          val accessor = e.getProperty(FIELD)
          accessor.setHistory(false, msgTime).set(value)
          e match {
            case _: Edge   =>
              par.getEdgeMgr.addProperty(e.getLocalId, FIELD, accessor)
            case _: Vertex =>
              par.getVertexMgr.addProperty(e.getLocalId, FIELD, accessor)
          }
        }
      case MutableInteger(key, value)  =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case MutableDouble(key, value)   =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case MutableFloat(key, value)    =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case MutableBoolean(key, value)  =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case _                           =>
    }

  // This method should assume that both vertices are local and create them if they don't exist
  override def addLocalEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
//    logger.trace(s"Adding edge: $srcId -> $dstId to partition: $partition @ t:$msgTime")
//    updateAdders(msgTime) // add source vertex
    val src = addVertexInternal(srcId, msgTime, Properties()) // handle dst
    val dst = localEntityStore.getLocalNodeId(dstId)

    val matchingEdges = src.findAllOutgoingEdges(dst, false)
    if (matchingEdges.hasNext) {
      matchingEdges.next()
      val foundEdge = matchingEdges.getEdge
      addVertexInternal(dstId, msgTime, Properties())
      updateExistingEdge(msgTime, properties, foundEdge)
    }
    else {
      vmgr.addHistory(dst, msgTime, true, properties.properties.nonEmpty, -1, false)
      addLocalVerticesToEdge(src.getLocalId, dst, msgTime, properties)
    }
  }

  // This method should assume that the dstId belongs to another partition
  override def addOutgoingEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {
    updateAdders(msgTime) // add source vertex
    val src = addVertexInternal(srcId, msgTime, Properties()) // handle dst

    val matchingEdges = src.findAllOutgoingEdges(srcId, true)
    if (matchingEdges.hasNext) {
      matchingEdges.next()
      val foundEdge = matchingEdges.getEdge
      updateExistingEdge(msgTime, properties, foundEdge)
    }
    else
      addRemoteOutgoingEdge(src, dstId, msgTime, properties)
  }

  override def addIncomingEdge(
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Unit = {

    updateAdders(msgTime)
    val dst = addVertexInternal(dstId, msgTime, properties)

    val matchingEdges = dst.findAllIncomingEdges(srcId, true)

    if (matchingEdges.hasNext) {
      matchingEdges.next()
      val foundEdge = matchingEdges.getEdge
      updateExistingEdge(msgTime, properties, foundEdge)
    }
    else
      addRemoteEdgeInternal(msgTime, srcId, dst, properties)
  }

  private def updateExistingEdge(
      msgTime: Long,
      properties: Properties,
      e: Edge
  ) = {
    // if destination is local add it
    // add the edge properties
    addOrUpdateEdgeProps(msgTime, e, properties)
    emgr.addHistory(e.getLocalId, msgTime, true, properties.properties.nonEmpty)
  }

  private def createVertex(localId: Long, globalId: Long, time: Long, properties: Properties): Vertex = {
    val v = par.getVertex
    v.reset(localId, globalId, true, time)
    addOrUpdateVertexProperties(time, v, properties)
    par.getVertexMgr.addVertex(v)
    vmgr.addHistory(v.getLocalId, time, true, properties.properties.nonEmpty, -1, false)
    v
  }

  private def addRemoteIncomingEdge(globalSrcId: Long, dst: Vertex, time: Long, properties: Properties): Unit =
    /*this.synchronized */{
      val e = par.getEdge
      e.init(par.getEdgeMgr.getNextFreeEdgeId, true, time)
      e.resetEdgeData(globalSrcId, dst.getLocalId, -1L, -1L, true, false)
      addOrUpdateEdgeProps(time, e, properties)
      par.getEdgeMgr.addEdge(e, -1L, -1L)
      par.getEdgeMgr.addHistory(e.getLocalId, time, true, true)
      val p = partitionFromVertex(dst)

      val id = /*p.synchronized*/ {
        p.addHistory(dst.getLocalId, time, true, false, e.getLocalId, false)
        p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId, e.getSrcVertex)
      }

      par.getEdgeMgr.setIncomingEdgePtr(e.getLocalId, id)
    }

  private def addRemoteOutgoingEdge(src: Vertex, globalDstId: Long, time: Long, properties: Properties): Unit =
    /*this.synchronized */{
      val e  = par.getEdge
      e.init(par.getEdgeMgr.getNextFreeEdgeId, true, time)
      e.resetEdgeData(src.getLocalId, globalDstId, -1L, -1L, false, true)
      addOrUpdateEdgeProps(time, e, properties)
      par.getEdgeMgr.addEdge(e, -1L, -1L)
      par.getEdgeMgr.addHistory(e.getLocalId, time, true, true)
      val p  = partitionFromVertex(src)
      val id = /*p.synchronized*/ {
        p.addHistory(src.getLocalId, time, true, false, e.getLocalId, true)
        p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, e.isDstGlobal)
      }
      par.getEdgeMgr.setOutgoingEdgePtr(e.getLocalId, id)
    }

  private def addLocalVerticesToEdge(srcLocalId: Long, dstLocalId: Long, time: Long, properties: Properties): Unit =
    /*this.synchronized*/ {
      val e   = par.getEdge
      e.init(par.getEdgeMgr.getNextFreeEdgeId, true, time)
      e.resetEdgeData(srcLocalId, dstLocalId, -1L, -1L, false, false)
      addOrUpdateEdgeProps(time, e, properties)
      par.getEdgeMgr.addEdge(e, -1L, -1L)
      par.getEdgeMgr.addHistory(e.getLocalId, time, true, true)
      var p   = partitionFromVertex2(srcLocalId)
      val id1 = /*p.synchronized */{
        p.addHistory(srcLocalId, time, true, false, e.getLocalId, true)
        p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, e.isDstGlobal)
      }

      par.getEdgeMgr.setOutgoingEdgePtr(e.getLocalId, id1)

      p = partitionFromVertex2(dstLocalId)
      val id = /*p.synchronized */{
        p.addHistory(dstLocalId, time, true, false, e.getLocalId, false)
        p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId, e.getSrcVertex)
      }

      par.getEdgeMgr.setIncomingEdgePtr(e.getLocalId, id)
    }

  private def addOrUpdateEdgeProps(msgTime: Long, e: Edge, properties: Properties): Unit =
    setProps(e, msgTime, properties)(key => par.getEdgePropertyId(key.toLowerCase()))(key => par.getEdgeFieldId(key))

  private def addOrUpdateVertexProperties(msgTime: Long, v: Vertex, properties: Properties): Unit =
    setProps(v, msgTime, properties)(key => par.getVertexPropertyId(key.toLowerCase()))(key =>
      par.getVertexFieldId(key.toLowerCase())
    )

  // This method should assume that the srcId belongs to another partition

  private def addRemoteEdgeInternal(msgTime: Long, srcId: Long, dst: Vertex, properties: Properties): Unit =
    addRemoteIncomingEdge(srcId, dst, msgTime, properties)

  private def getIncomingEdge(srcId: Long, dst: Vertex): Option[Edge] = {
    val iter0 = dst.getIncomingEdges
    while (iter0.hasNext) {
      val eId = iter0.next()
      if (iter0.getSrcVertexId == srcId && !iter0.isSrcVertexLocal)
        return Some(iter0.getEdge)
    }
    None
  }

  override def getVertices(graphPerspective: LensInterface, start: Long, end: Long): mutable.Map[Long, PojoExVertex] =
    ???

  override def close(): Unit = par.close()
}

object ArrowPartition {

  class PropertyIterator[P](iter: ArrowPropertyIterator)(implicit P: Prop[P]) extends Iterator[Option[(P, Long)]] {
    override def hasNext: Boolean = iter.hasNext

    override def next(): Option[(P, Long)] = {
      val acc = iter.next()
      P.get(acc).map(p => p -> acc.getCreationTime)
    }
  }

  class VertexIterator(vs: com.raphtory.arrowcore.implementation.VertexIterator) extends Iterator[Vertex] {

    override def hasNext: Boolean =
      vs.hasNext

    override def next(): Vertex = {
      vs.next()
      vs.getVertex
    }
  }

  class EdgesIterator(es: EdgeIterator) extends Iterator[Edge] {

    override def hasNext: Boolean =
      es.hasNext

    override def next(): Edge = {
      es.next()
      es.getEdge
    }

  }

  class VertexHistoryIterator(vhi: VertexHistoryIterator.WindowedVertexHistoryIterator)
          extends Iterator[HistoricEvent] {
    override def hasNext: Boolean = vhi.hasNext

    override def next(): HistoricEvent = {
      vhi.next()
      HistoricEvent(vhi.getModificationTime, vhi.getModificationTime, vhi.wasActive())
    }

  }

  class EdgeHistoryIterator(vhi: EdgeHistoryIterator.WindowedEdgeHistoryIterator) extends Iterator[HistoricEvent] {
    override def hasNext: Boolean = vhi.hasNext

    override def next(): HistoricEvent = {
      vhi.next()
      HistoricEvent(vhi.getModificationTime, vhi.getModificationTime, vhi.wasActive())
    }

  }

  class MatchingEdgesIterator(mei: EdgeIterator) extends Iterator[Edge] {
    override def hasNext: Boolean = mei.hasNext

    override def next(): Edge = {
      mei.next()
      mei.getEdge
    }
  }

  def apply(graphId: String, cfg: ArrowPartitionConfig, config: Config): ArrowPartition = {
    val arrowConfig = new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig)

    new ArrowPartition(graphId, arrowConfig, arrowConfig.getRaphtoryPartitionId, config)
  }
}
