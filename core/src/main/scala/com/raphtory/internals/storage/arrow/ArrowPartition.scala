package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.visitor
import com.raphtory.api.input.BooleanProperty
import com.raphtory.api.input.DoubleProperty
import com.raphtory.api.input.FloatProperty
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.IntegerProperty
import com.raphtory.api.input.LongProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.StringProperty
import com.raphtory.api.input.Type
import com.raphtory.arrowcore.implementation.VertexIterator.AllVerticesIterator
import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EdgePartitionManager
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.graph.GraphAlteration.EdgeSyncAck
import com.raphtory.internals.graph.GraphAlteration.SyncExistingEdgeAdd
import com.raphtory.internals.graph.GraphAlteration.SyncExistingRemovals
import com.raphtory.internals.graph.GraphAlteration.SyncNewEdgeAdd
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config

import java.lang
import java.util.concurrent.atomic.LongAccumulator
import java.util.concurrent.atomic.LongAdder
import scala.collection.AbstractView
import scala.collection.View
import scala.collection.mutable

class ArrowPartition(val par: RaphtoryArrowPartition, graphID: String, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf) {

  def getVertex(id: Long): Vertex =
    par.getVertexMgr.getVertex(id)

  def vertexCount: Int = par.getVertexMgr.getTotalNumberOfVertices.toInt

  val min: LongAccumulator = new LongAccumulator(Math.min(_, _), Long.MaxValue)
  val max: LongAccumulator = new LongAccumulator(Math.max(_, _), Long.MinValue)

  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val totalPartitions: Int     = partitionServers * partitionsPerServer

  private val idsRepo = new LocalEntityRepo(par.getLocalEntityIdStore, totalPartitions, partition)

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
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Option[Type]
  ): Unit =
    addVertexInternal(srcId, msgTime, properties)

  private def updateAdders(msgTime: Long): Unit = {
    min.accumulate(msgTime)
    max.accumulate(msgTime)
  }

  private def addVertexInternal(srcId: Long, msgTime: Long, properties: Properties): Vertex = {

    updateAdders(msgTime)

    val vertex = idsRepo.resolve(srcId) match {
      case NotFound(id)          => // it's not present on this partition .. yet
        val srcLocalId = vmgr.getNextFreeVertexId
        val v          = par.getVertex
        v.reset(srcLocalId, id, true, msgTime)

        properties.properties.foreach {
          case ImmutableProperty(key, value) =>
            val FIELD = par.getVertexFieldId(key.toLowerCase())
            v.getField(FIELD).set(new lang.StringBuilder(value))
          case StringProperty(key, value)    =>
            val FIELD = par.getVertexPropertyId(key.toLowerCase())
            v.getProperty(FIELD).setHistory(true, msgTime).set(new lang.StringBuilder(value))
          case LongProperty(key, value)      =>
            val FIELD = par.getVertexPropertyId(key.toLowerCase())
            v.getProperty(FIELD).setHistory(true, msgTime).set(value)
          case IntegerProperty(key, value)   =>
            val FIELD = par.getVertexPropertyId(key.toLowerCase())
            v.getProperty(FIELD).setHistory(true, msgTime).set(value)
          case DoubleProperty(key, value)    =>
            val FIELD = par.getVertexPropertyId(key.toLowerCase())
            v.getProperty(FIELD).setHistory(true, msgTime).set(value)
          case FloatProperty(key, value)     =>
            val FIELD = par.getVertexPropertyId(key.toLowerCase())
            v.getProperty(FIELD).setHistory(true, msgTime).set(value)
          case BooleanProperty(key, value)   =>
            val FIELD = par.getVertexPropertyId(key.toLowerCase())
            v.getProperty(FIELD).setHistory(true, msgTime).set(value)
          case _                             =>
        }

        vmgr.addVertex(v)
        // update is true or false?
        vmgr.addHistory(v.getLocalId, msgTime, true, false, -1, false)
        v
      case ExistsOnPartition(id) => // we've seen you before but are there any differences?
        // TODO: need a way to merge properties into existing vertex
        // TODO: we can't just overwrite a vertex that already exists
        // we make sure the vertex is active at this point
//        vmgr.addHistory(v.getLocalId, msgTime, true, true, -1, false)
        val v = vmgr.getVertex(id)
        if (v.getOldestPoint != msgTime) // weak check to avoid adding multiple duplicated timestamps
          vmgr.addHistory(v.getLocalId, msgTime, true, false, -1, false)

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

    updateAdders(msgTime)
    // add source vertex
    val src = addVertexInternal(srcId, msgTime, Properties())
    // handle dst
    val dst = idsRepo.resolve(dstId)

    src.outgoingEdges.find { e =>
      dst match {
        case NotFound(_)           => false
        case GlobalId(id)          => id == e.getDstVertex
        case ExistsOnPartition(id) => id == e.getDstVertex
      }
    } match {
      case Some(e) =>
        // edge exists so we activate the history
        par.getEdgeMgr.addHistory(e.getLocalId, msgTime, true)
        // if destination is local add it
        if (dst.isLocal) {
          addVertexInternal(dstId, msgTime, Properties())
          None
        }
        else
          // send sync
          Some(SyncExistingEdgeAdd(sourceID, msgTime, index, srcId, dstId, properties))
      case None    =>
        // init the edge
        val eId = emgr.getNextFreeEdgeId
        val e   = par.getEdge
        e.init(eId, true, msgTime)

        val out = if (dst.isLocal) { // do we belong here?
          // add dst vertex if local
          addVertexInternal(dstId, msgTime, Properties())
          None
        }
        else
          // otherwise sync new edge
          Some(SyncNewEdgeAdd(sourceID, msgTime, index, srcId, dstId, properties, Nil, edgeType))

        // add the edge properties
        properties.properties.foreach {
          case ImmutableProperty(key, value) =>
            val FIELD = par.getEdgeFieldId(key)
            e.getField(FIELD).set(new java.lang.StringBuilder(value))
          case LongProperty(key, value)      =>
            val FIELD = par.getEdgePropertyId(key)
            e.getProperty(FIELD).setHistory(true, msgTime).set(value)
        }

        // add the actual edge
        e.resetEdgeData(src.getLocalId, dst.id, -1L, -1L, false, dst.isGlobal)
        emgr.addEdge(e, -1L, -1L)
        emgr.addHistory(e.getLocalId, msgTime, true)

        //link the edge to the source
        val p = vmgr.getPartition(vmgr.getPartitionId(src.getLocalId))

        val prevListPtr = p.synchronized {
          val ptr = p.addOutgoingEdgeToList(src.getLocalId, e.getLocalId)
          p.addHistory(src.getLocalId, msgTime, true, false, e.getLocalId, true)
          ptr
        }
        emgr.setOutgoingEdgePtr(e.getLocalId, prevListPtr)

        // link the edge to the destination
        if (dst.isLocal && checkDst(dst.id)) {
          val p           = vmgr.getPartition(vmgr.getPartitionId(dst.id))
          val prevListPtr = p.synchronized {
            val ptr = p.addIncomingEdgeToList(dst.id, e.getLocalId)
            p.addHistory(dst.id, msgTime, true, false, e.getLocalId, false)
            ptr
          }

          emgr.setIncomingEdgePtr(e.getLocalId, prevListPtr)
        }
        out
      // no edge here
    }
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
  ): GraphAlteration.GraphUpdateEffect = {

    updateAdders(msgTime)

    val dst = addVertexInternal(dstId, msgTime, properties)

    getIncomingEdge(srcId, dst) match {
      case Some(e) =>
        // activate edge
        // FIXME: what about properties?
        emgr.addHistory(e.getLocalId, msgTime, true)
      case None    =>
        addRemoteEdgeInternal(msgTime, srcId, dst, properties)
    }

    SyncExistingRemovals(
            sourceID = sourceID,
            updateTime = msgTime,
            index = index,
            srcId = srcId,
            dstId = dstId,
            removals = Nil,
            fromAddition = true
    )
  }

  private def addRemoteEdgeInternal(msgTime: Long, srcId: Long, dst: Vertex, properties: Properties): Unit = {
    updateAdders(msgTime)
    // init the edge
    val eId = emgr.getNextFreeEdgeId
    val e   = par.getEdge
    e.init(eId, true, msgTime)

    // handle src
    val src = idsRepo.resolve(srcId)

    assert(src.isGlobal)

    // add the edge properties
    properties.properties.foreach {
      case ImmutableProperty(key, value) =>
        val FIELD = par.getEdgeFieldId(key)
        e.getField(FIELD).set(value)
      case LongProperty(key, value)      =>
        val FIELD = par.getEdgePropertyId(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
    }

    // add the actual edge
    e.resetEdgeData(src.id, dst.getLocalId, -1L, -1L, src.isGlobal, false)
    emgr.addEdge(e, -1L, -1L)
    emgr.addHistory(e.getLocalId, msgTime, true)

    // link edge to the destination
    val p           = vmgr.getPartition(vmgr.getPartitionId(dst.getLocalId))
    val prevListPtr = p.synchronized {
      val ptr = p.addIncomingEdgeToList(dst.getLocalId, e.getLocalId)
      p.addHistory(dst.getLocalId, msgTime, true, false, e.getLocalId, false)
      ptr
    }
    emgr.setIncomingEdgePtr(e.getLocalId, prevListPtr)
  }

  private def getIncomingEdge(srcId: Long, dst: Vertex): Option[Edge] =
    dst.incomingEdges.find(e => e.getSrcVertex == srcId && e.isSrcGlobal)

  override def syncExistingEdgeAdd(
      sourceID: Int,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): GraphAlteration.GraphUpdateEffect = {

    updateAdders(msgTime)

    val dst = addVertexInternal(dstId, msgTime, properties)

    getIncomingEdge(srcId, dst) match {
      case Some(e) =>
        emgr.addHistory(e.getLocalId, msgTime, true)
      case None    =>
        addRemoteEdgeInternal(msgTime, srcId, dst, properties)
    }

    EdgeSyncAck(sourceID, msgTime, index, srcId, dstId, fromAddition = true)
  }

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
  ): Unit = {}

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

  class VertexIterator(vs: com.raphtory.arrowcore.implementation.VertexIterator) extends Iterator[Vertex] {

    override def hasNext: Boolean =
      vs.hasNext

    override def next(): Vertex = {
      vs.next()
      vs.getVertex
    }
  }

  class EdgesIterator(es: EdgeIterator) extends Iterator[Edge] {
    override def hasNext: Boolean = es.hasNext

    override def next(): Edge = {
      es.next()
      es.getEdge
    }

  }

  def apply(cfg: ArrowPartitionConfig, config: Config): ArrowPartition = {
    val graphID     = config.getString("raphtory.graph.id")
    val arrowConfig = new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig)

    new ArrowPartition(arrowConfig, graphID, arrowConfig.getRaphtoryPartitionId, config)
  }
}
