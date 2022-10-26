package com.raphtory.internals.storage.arrow

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.input._
import com.raphtory.arrowcore.implementation.ArrowPropertyIterator
import com.raphtory.arrowcore.implementation.EdgeHistoryIterator
import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EdgePartitionManager
import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexHistoryIterator
import com.raphtory.arrowcore.implementation.VertexPartition
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.graph.GraphAlteration.EdgeSyncAck
import com.raphtory.internals.graph.GraphAlteration.SyncExistingEdgeAdd
import com.raphtory.internals.graph.GraphAlteration.SyncExistingEdgeRemoval
import com.raphtory.internals.graph.GraphAlteration.SyncExistingRemovals
import com.raphtory.internals.graph.GraphAlteration.SyncNewEdgeAdd
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex
import com.typesafe.config.Config
import com.raphtory.internals.communication.SchemaProviderInstances._

import java.lang
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
      sourceID: Long,
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

    idsRepo.resolve(srcId) match {
      case NotFound(id)          => // it's not present on this partition .. yet
        val srcLocalId = vmgr.getNextFreeVertexId
        createVertex(srcLocalId, srcId, msgTime, properties)
      case ExistsOnPartition(id) => // we've seen you before but are there any differences?
        // TODO: need a way to merge properties into existing vertex
        // TODO: we can't just overwrite a vertex that already exists
        // we make sure the vertex is active at this point
        val v                  = vmgr.getVertex(id)
//
        val props: Set[String] =
          par.getPropertySchema.versionedVertexProperties().asScala.map(_.name()).toSet intersect properties.properties
            .map(_.key.toLowerCase())
            .toSet

        addOrUpdateVertexProperties(
                msgTime,
                v,
                Properties(properties.properties.filter(p => props(p.key.toLowerCase())): _*)
        )
//
//        if (v.getOldestPoint != msgTime) // FIXME: weak check to avoid adding multiple duplicated timestamps
//        val vPar = partitionFromVertex(v)
//          FIXME: this breaks history

        vmgr.addHistory(v.getLocalId, msgTime, true, properties.properties.nonEmpty, -1, false)
        v
      case _                     => throw new IllegalStateException(s"Node $srcId does not belong to partition $getPartitionID")
    }

  }

  private def partitionFromVertex(v: Vertex) =
    par.getVertexMgr.getPartition(par.getVertexMgr.getPartitionId(v.getLocalId))

  private def setProps(e: Entity, msgTime: Long, properties: Properties)(
      lookupProp: String => Int
  )(lookupField: String => Int): Unit =
    properties.properties.foreach {
      case ImmutableProperty(key, value) =>
        val FIELD    = lookupField(key.toLowerCase())
        val accessor = e.getField(FIELD)
        accessor.set(new lang.StringBuilder(value))
      case StringProperty(key, value)    =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(new lang.StringBuilder(value))
      case LongProperty(key, value)      =>
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
      case IntegerProperty(key, value)   =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case DoubleProperty(key, value)    =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case FloatProperty(key, value)     =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case BooleanProperty(key, value)   =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case _                             =>
    }

  override def addEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Option[Type]
  ): Option[GraphAlteration.GraphUpdateEffect] = {

    logger.trace(s"Adding edge: $srcId -> $dstId to partition: $partition @ t:$msgTime")

    updateAdders(msgTime)
    // add source vertex
    val src = addVertexInternal(srcId, msgTime, Properties())
    // handle dst
    val dst = idsRepo.resolve(dstId)

    src.outgoingEdges.find { e =>
      dst match {
        case NotFound(_)           => false
        case GlobalId(id)          => id == e.getDstVertex && e.isDstGlobal
        case ExistsOnPartition(id) => id == e.getDstVertex && !e.isDstGlobal
      }
    } match {
      case Some(e) =>
        updateExistingEdge(sourceID, msgTime, index, srcId, dstId, properties, dst, e)
      case None    =>
        if (dst.isLocal) {
          val dstV = addVertexInternal(dstId, msgTime, Properties())
          addLocalVerticesToEdge(src, dstV, msgTime, properties)
          None
        }
        else {
          addRemoteOutgoingEdge(src, dst.id, msgTime, properties)

          Some(SyncNewEdgeAdd(sourceID, msgTime, index, srcId, dstId, properties, Nil, edgeType))
        }

    }
  }

  private def updateExistingEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      dst: EntityId,
      e: Edge
  ) = {
    // if destination is local add it
    // add the edge properties
    addOrUpdateEdgeProps(msgTime, e, properties)
    emgr.addHistory(e.getLocalId, msgTime, true, properties.properties.nonEmpty)
    if (dst.isLocal) {
      // if destination is local add it
      addVertexInternal(dstId, msgTime, Properties())
      None
    }
    else // send sync
      Some(SyncExistingEdgeAdd(sourceID, msgTime, index, srcId, dstId, properties))
  }

  private def createVertex(localId: Long, globalId: Long, time: Long, properties: Properties): Vertex = {
    val v = par.getVertex
    v.reset(localId, globalId, true, time)
    addOrUpdateVertexProperties(time, v, properties)
    par.getVertexMgr.addVertex(v)
    vmgr.addHistory(v.getLocalId, time, true, properties.properties.nonEmpty, -1, false)
    v
  }

  private def addRemoteIncomingEdge(globalSrcId: Long, dst: Vertex, time: Long, properties: Properties): Unit = {
    logger.trace(
            s"PAR $partition ADD Remote incoming G($globalSrcId) -> (${dst.getGlobalId}:${dst.getLocalId}) @ $time"
    )
    val e = par.getEdge
    e.init(par.getEdgeMgr.getNextFreeEdgeId, true, time)
    e.resetEdgeData(globalSrcId, dst.getLocalId, -1L, -1L, true, false)
    addOrUpdateEdgeProps(time, e, properties)
    par.getEdgeMgr.addEdge(e, -1L, -1L)
    par.getEdgeMgr.addHistory(e.getLocalId, time, true, true)
    val p = partitionFromVertex(dst)
    par.getEdgeMgr.setIncomingEdgePtr(e.getLocalId, p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId))
    p.addHistory(dst.getLocalId, time, true, false, e.getLocalId, false)
  }

  private def addRemoteOutgoingEdge(src: Vertex, globalDstId: Long, time: Long, properties: Properties): Unit = {
    logger.trace(
            s"PAR $partition ADD Remote Outgoing -> (${src.getGlobalId}:${src.getLocalId}) -> G($globalDstId)  @ $time"
    )
    val e = par.getEdge
    e.init(par.getEdgeMgr.getNextFreeEdgeId, true, time)
    e.resetEdgeData(src.getLocalId, globalDstId, -1L, -1L, false, true)
    addOrUpdateEdgeProps(time, e, properties)
    par.getEdgeMgr.addEdge(e, -1L, -1L)
    par.getEdgeMgr.addHistory(e.getLocalId, time, true, true)
    val p = partitionFromVertex(src)
    par.getEdgeMgr
      .setOutgoingEdgePtr(e.getLocalId, p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, e.isDstGlobal))
//      .setOutgoingEdgePtr(e.getLocalId, p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex))
    p.addHistory(src.getLocalId, time, true, false, e.getLocalId, true)
  }

  private def addLocalVerticesToEdge(src: Vertex, dst: Vertex, time: Long, properties: Properties): Unit = {
    logger.trace(s"PAR $partition ADD Local Edge -> ${src.getLocalId} -> ${dst.getLocalId}  @ $time")
    val e = par.getEdge
    e.init(par.getEdgeMgr.getNextFreeEdgeId, true, time)
    e.resetEdgeData(src.getLocalId, dst.getLocalId, -1L, -1L, false, false)
    addOrUpdateEdgeProps(time, e, properties)
    par.getEdgeMgr.addEdge(e, -1L, -1L)
    par.getEdgeMgr.addHistory(e.getLocalId, time, true, true)
    var p = partitionFromVertex(src)
    par.getEdgeMgr
      .setOutgoingEdgePtr(e.getLocalId, p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, e.isDstGlobal))
    p.addHistory(src.getLocalId, time, true, false, e.getLocalId, true)
    p = partitionFromVertex(dst)
    par.getEdgeMgr.setIncomingEdgePtr(e.getLocalId, p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId))
    p.addHistory(dst.getLocalId, time, true, false, e.getLocalId, false)
  }

  private def addOrUpdateEdgeProps(msgTime: Long, e: Edge, properties: Properties): Unit =
    setProps(e, msgTime, properties)(key => par.getEdgePropertyId(key.toLowerCase()))(key => par.getEdgeFieldId(key))

  private def addOrUpdateVertexProperties(msgTime: Long, v: Vertex, properties: Properties): Unit =
    setProps(v, msgTime, properties)(key => par.getVertexPropertyId(key.toLowerCase()))(key =>
      par.getVertexFieldId(key.toLowerCase())
    )

  private def linkIncomingToLocalNode(msgTime: Long, dst: EntityId, e: Edge): Unit = {
    val p           = vmgr.getPartition(vmgr.getPartitionId(dst.id))
    val prevListPtr = p.synchronized {
      val ptr = p.addIncomingEdgeToList(dst.id, e.getLocalId)
      p.addHistory(dst.id, msgTime, true, false, e.getLocalId, false)
      ptr
    }

    emgr.setIncomingEdgePtr(e.getLocalId, prevListPtr)
  }

  override def syncNewEdgeAdd(
      sourceID: Long,
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
        logger.trace(s"Updating existing edge: $srcId -> $dstId to partition: $partition @ t:$msgTime")
        // activate edge
        // FIXME: what about properties?

        val props: Set[String] =
          par.getPropertySchema.versionedEdgeProperties().asScala.map(_.name()).toSet intersect properties.properties
            .map(_.key.toLowerCase())
            .toSet

        addOrUpdateEdgeProps(
                msgTime,
                e,
                Properties(properties.properties.filter(p => props(p.key.toLowerCase())): _*)
        )

        emgr.addHistory(e.getLocalId, msgTime, true, properties.properties.nonEmpty)
      case None    =>
        logger.trace(s"Adding inbound edge: $srcId -> $dstId to partition: $partition @ t:$msgTime")
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

  private def addRemoteEdgeInternal(msgTime: Long, srcId: Long, dst: Vertex, properties: Properties): Unit =
    addRemoteIncomingEdge(srcId, dst, msgTime, properties)

  private def getIncomingEdge(srcId: Long, dst: Vertex): Option[Edge] =
    dst.incomingEdges.find(e => e.getSrcVertex == srcId && e.isSrcGlobal)

  override def syncExistingEdgeAdd(
      sourceID: Long,
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
        val props: Set[String] =
          par.getPropertySchema.versionedEdgeProperties().asScala.map(_.name()).toSet intersect properties.properties
            .map(_.key.toLowerCase())
            .toSet

        addOrUpdateEdgeProps(
                msgTime,
                e,
                Properties(properties.properties.filter(p => props(p.key.toLowerCase())): _*)
        )

        emgr.addHistory(e.getLocalId, msgTime, true, properties.properties.nonEmpty)
      case None    =>
        addRemoteEdgeInternal(msgTime, srcId, dst, properties)
    }

    EdgeSyncAck(sourceID, msgTime, index, srcId, dstId, fromAddition = true)
  }

  override def removeEdge(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): Option[GraphAlteration.GraphUpdateEffect] = {

    val src = idsRepo.resolve(srcId)
    val dst = idsRepo.resolve(dstId)

    val v = vmgr.getVertex(src.id)

    val edgeFound = v.outgoingEdges(dst.id, dst.isGlobal).foldLeft(false) { (updated, e) =>
      emgr.addHistory(e.getLocalId, msgTime, false, false)
      true // we found at least one edge and changed it
    }

    if (edgeFound)
      if (dst.isLocal)
        None
      else
        Some(SyncExistingEdgeRemoval(sourceID, msgTime, index, srcId, dstId))
    else None
  }

  override def syncNewEdgeRemoval(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long,
      srcRemovals: List[(Long, Long)]
  ): GraphAlteration.GraphUpdateEffect = ???

  override def syncExistingEdgeRemoval(
      sourceID: Long,
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
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long
  ): List[GraphAlteration.GraphUpdateEffect] = ???

  override def inboundEdgeRemovalViaVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphAlteration.GraphUpdateEffect = ???

  override def outboundEdgeRemovalViaVertex(
      sourceID: Long,
      msgTime: Long,
      index: Long,
      srcId: Long,
      dstId: Long
  ): GraphAlteration.GraphUpdateEffect = ???

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

  class MatchingEdgesIterator(mei: EdgeIterator.MatchingEdgesIterator) extends Iterator[Edge] {
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
