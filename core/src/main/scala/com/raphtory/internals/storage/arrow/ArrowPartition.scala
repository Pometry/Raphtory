package com.raphtory.internals.storage.arrow

import com.raphtory.api.input._
import com.raphtory.arrowcore.implementation.ArrowPropertyIterator
import com.raphtory.arrowcore.implementation.EdgeIterator
import com.raphtory.arrowcore.implementation.EdgePartitionManager
import com.raphtory.arrowcore.implementation.EntityFieldAccessor
import com.raphtory.arrowcore.implementation.RaphtoryArrowPartition
import com.raphtory.arrowcore.implementation.VertexPartitionManager
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Entity
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

import com.raphtory.internals.communication.SchemaProviderInstances._

import java.lang
import java.util.concurrent.atomic.LongAccumulator
import scala.collection.AbstractView
import scala.collection.View
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ArrowPartition(val par: RaphtoryArrowPartition, graphID: String, partition: Int, conf: Config)
        extends GraphPartition(graphID, partition, conf) {

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

    val vertex = idsRepo.resolve(srcId) match {
      case NotFound(id)          => // it's not present on this partition .. yet
        val srcLocalId = vmgr.getNextFreeVertexId
        val v          = par.getVertex
        v.reset(srcLocalId, id, true, msgTime)

        addOrUpdateVertexProperties(msgTime, v, properties)

        vmgr.addVertex(v)
        // update is true or false?
        vmgr.addHistory(v.getLocalId, msgTime, true, false, -1, false)
        v
      case ExistsOnPartition(id) => // we've seen you before but are there any differences?
        // TODO: need a way to merge properties into existing vertex
        // TODO: we can't just overwrite a vertex that already exists
        // we make sure the vertex is active at this point
        val v = vmgr.getVertex(id)

        val props: Set[String] =
          par.getPropertySchema.versionedVertexProperties().asScala.map(_.name()).toSet intersect properties.properties
            .map(_.key.toLowerCase())
            .toSet

        addOrUpdateVertexProperties(
                msgTime,
                v,
                Properties(properties.properties.filter(p => props(p.key.toLowerCase())): _*)
        )

        if (v.getOldestPoint != msgTime) // FIXME: weak check to avoid adding multiple duplicated timestamps
          vmgr.addHistory(v.getLocalId, msgTime, true, false, -1, false)

        v
      case _                     => throw new IllegalStateException(s"Node $srcId does not belong to partition $getPartitionID")
    }

    vertex
  }


  private def setProps(v: Entity, msgTime: Long, properties: Properties)(
      lookupProp: String => Int
  )(lookupField: String => Int): Unit =
    properties.properties.foreach {
      case ImmutableProperty(key, value) =>
        val FIELD = lookupField(key.toLowerCase())
        v.getField(FIELD).set(new lang.StringBuilder(value))
      case StringProperty(key, value)    =>
        val FIELD = lookupProp(key)
        v.getProperty(FIELD).setHistory(true, msgTime).set(new lang.StringBuilder(value))
      case LongProperty(key, value)      =>
        val FIELD = lookupProp(key)
        if (!v.getProperty(FIELD).isSet)
          v.getProperty(FIELD).setHistory(true, msgTime).set(value)
        else {
          val accessor = v.getProperty(FIELD)
          accessor.setHistory(false, msgTime).set(value)

          v match {
            case _: Edge   =>
              par.getEdgeMgr.addProperty(v.getLocalId, FIELD, accessor)
            case _: Vertex =>
              par.getVertexMgr.addProperty(v.getLocalId, FIELD, accessor)
            case _         => ???
          }
        }
      case IntegerProperty(key, value)   =>
        val FIELD = lookupProp(key)
        v.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case DoubleProperty(key, value)    =>
        val FIELD = lookupProp(key)
        v.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case FloatProperty(key, value)     =>
        val FIELD = lookupProp(key)
        v.getProperty(FIELD).setHistory(true, msgTime).set(value)
      case BooleanProperty(key, value)   =>
        val FIELD = lookupProp(key)
        v.getProperty(FIELD).setHistory(true, msgTime).set(value)
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
        // edge exists so we activate the history
        par.getEdgeMgr.addHistory(e.getLocalId, msgTime, true)
        // if destination is local add it
        emgr.addHistory(e.getLocalId, msgTime, true)
        // add the edge properties
        addOrUpdateEdgeProps(msgTime, e, properties)
        if (dst.isLocal) {
          val d = addVertexInternal(dstId, msgTime, Properties())
//          linkIncomingToLocalNode(msgTime, dst, e)
          None
        }
        else // send sync
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
        addOrUpdateEdgeProps(msgTime, e, properties)

        // add the actual edge
        e.resetEdgeData(src.getLocalId, dst.id, -1L, -1L, false, dst.isGlobal)
        emgr.addEdge(e, -1L, -1L)
        emgr.addHistory(e.getLocalId, msgTime, true)

        //link the edge to the source
        val p = vmgr.getPartition(vmgr.getPartitionId(src.getLocalId))

        val prevListPtr = p.synchronized {
          val ptr = p.addOutgoingEdgeToList(src.getLocalId, e.getLocalId, dst.id)
          p.addHistory(src.getLocalId, msgTime, true, false, e.getLocalId, true)
          ptr
        }
        emgr.setOutgoingEdgePtr(e.getLocalId, prevListPtr)

        // link the edge to the destination
        if (dst.isLocal)
          linkIncomingToLocalNode(msgTime, dst, e)
        out
      // no edge here
    }
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
    addOrUpdateEdgeProps(msgTime, e, properties)

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

        emgr.addHistory(e.getLocalId, msgTime, true)
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
  ): Option[GraphAlteration.GraphUpdateEffect] = ???

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
}

object ArrowPartition {

  class PropertyIterator[P](iter: ArrowPropertyIterator)(implicit P: Prop[P]) extends Iterator[(P, Long)] {
    override def hasNext: Boolean = iter.hasNext

    override def next(): (P, Long) = {
      val acc = iter.next()
      P.get(acc) -> acc.getCreationTime
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

    override def hasNext: Boolean = {
      val next1 = es.hasNext
      next1
    }

    override def next(): Edge = {
      es.next()
      val edge = es.getEdge
      edge
    }

  }

  def apply(cfg: ArrowPartitionConfig, config: Config): ArrowPartition = {
    val graphID     = config.getString("raphtory.graph.id")
    val arrowConfig = new RaphtoryArrowPartition(cfg.toRaphtoryPartitionConfig)

    new ArrowPartition(arrowConfig, graphID, arrowConfig.getRaphtoryPartitionId, config)
  }
}
