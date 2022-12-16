package com.raphtory.internals.storage.arrow

import com.lmax.disruptor.EventHandler
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.MutableBoolean
import com.raphtory.api.input.MutableDouble
import com.raphtory.api.input.MutableFloat
import com.raphtory.api.input.MutableInteger
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.MutableString
import com.raphtory.api.input.Properties
import com.raphtory.arrowcore.implementation.{EdgeIterator, EdgePartition, EntityFieldAccessor, RaphtoryArrowPartition, VertexIterator, VertexPartition}
import com.raphtory.arrowcore.model.Edge
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.Vertex
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.management.Partitioner
import com.typesafe.config.Config

private[raphtory] class Worker(private val _id: Int, _rap: RaphtoryArrowPartition, conf: Config)
        extends EventHandler[QueuePayload] {

  private val partitioner                        = Partitioner(conf)
  _vertexIter2 = _rap.getNewAllVerticesIterator
  private var _lastVertexId                      = -1L
  private var _endVertexId                       = -1L
  private var _lastEdgeId                        = -1L
  private var _endEdgeId                         = -1L
  final private var _vertexIter2: VertexIterator = _rap.getNewAllVerticesIterator
  private val _avpm                              = _rap.getVertexMgr
  private val _aepm                              = _rap.getEdgeMgr
  private var vertexCount                        = 0L
  private var edgeCount                          = 0L

  @throws[Exception]
  override def onEvent(av: QueuePayload, l: Long, b: Boolean): Unit =
    av match {
      case QueuePayload(vAdd: GraphAlteration.VertexAdd) =>
//        println(vAdd)
        addVertex(vAdd)
      case QueuePayload(eAdd: GraphAlteration.EdgeAdd)   =>
//        println(eAdd)
        addLocalEdge(eAdd)
    }

  private def addLocalEdge(av: GraphAlteration.EdgeAdd): Unit = {
    val srcId = _rap.getLocalEntityIdStore.getLocalNodeId(av.srcId)
    var dstId = 0L
    while ({ dstId = _rap.getLocalEntityIdStore.getLocalNodeId(av.dstId); dstId } == -1L) {
      Thread.`yield`() // we expect dst should show up eventually
    }
    // Check if edge already exists...
    var e          = -1L
    var edge: Edge = null
    _vertexIter2.reset(srcId)
    val iter       = _vertexIter2.findAllOutgoingEdges(dstId, false)
    if (iter.hasNext) {
      e = iter.next
      edge = iter.getEdge
    }
    ///System.out.println(av._globalId + "," + av._dstGlobalId + ", " + (e!=null));
    if (e == -1L)
      addLocalEdge(srcId, dstId, av.updateTime, av.properties)
    else {
      addOrUpdateEdgeProps(av.updateTime, edge, av.properties)
      _aepm.addHistory(e, av.updateTime, true, true)
      _avpm.addHistory(iter.getSrcVertexId, av.updateTime, true, false, e, true)
      _avpm.addHistory(iter.getDstVertexId, av.updateTime, true, false, e, false)
    }
  }

  private def addLocalEdge(srcId: Long, dstId: Long, time: Long, properties: Properties): Unit = {
    if (_lastEdgeId == -1L || _lastEdgeId >= _endEdgeId) {
      val partId = _aepm.getNewPartitionId
      _lastEdgeId = partId * _aepm.PARTITION_SIZE
      _endEdgeId = _lastEdgeId + _aepm.PARTITION_SIZE
    }
    val e = _rap.getEdge
    e.incRefCount()
    e.init(_lastEdgeId, true, time)
    e.resetEdgeData(srcId, dstId, false, false)

    addOrUpdateEdgeProps(time, e, properties)
    // add properties here
    _aepm.addEdge(e, -1L, -1L)
    val ep = _aepm.getPartition(_aepm.getPartitionId(e.getLocalId))
    ep.addHistory(e.getLocalId, time, true, true)
    var p  = _avpm.getPartitionForVertex(srcId)
    ep.setOutgoingEdgePtrByEdgeId(
            e.getLocalId,
            p.addOutgoingEdgeToList(e.getSrcVertex, e.getLocalId, e.getDstVertex, false)
    )
    p.addHistory(srcId, time, true, false, e.getLocalId, true)
    p = _avpm.getPartitionForVertex(dstId)
    ep.setIncomingEdgePtrByEdgeId(e.getLocalId, p.addIncomingEdgeToList(e.getDstVertex, e.getLocalId, e.getSrcVertex))
    p.addHistory(dstId, time, true, false, e.getLocalId, false)
    e.decRefCount()
    edgeCount += 1
    _lastEdgeId += 1
  }

  private def addVertex(av: VertexAdd): Unit = {
    if (_lastVertexId == -1L || _lastVertexId >= _endVertexId) {
      val partId = _avpm.getNewPartitionId
      _lastVertexId = partId * _avpm.PARTITION_SIZE
      _endVertexId = _lastVertexId + _avpm.PARTITION_SIZE
    }
    val localId = _rap.getLocalEntityIdStore.getLocalNodeId(av.srcId)
    if (localId == -1L) {
      createVertex(_lastVertexId, av.srcId, av.updateTime, av.properties).decRefCount()
      _lastVertexId += 1
    }
  }

  private def createVertex(localId: Long, globalId: Long, time: Long, properties: Properties): Vertex = {
    val v = _rap.getVertex
    v.reset(localId, globalId, true, time)
    addOrUpdateVertexProperties(time, v, properties)
    _rap.getVertexMgr.addVertex(v)
    _avpm.addHistory(v.getLocalId, time, true, properties.properties.nonEmpty, -1, false)
    vertexCount += 1
    v
  }

  private def addOrUpdateEdgeProps(msgTime: Long, e: Edge, properties: Properties): Unit =
    setProps(e, msgTime, properties)(key => getEdgePropertyId(key))(key => getEdgeFieldId(key))

  private def addOrUpdateVertexProperties(msgTime: Long, v: Vertex, properties: Properties): Unit =
    setProps(v, msgTime, properties)(key => getVertexPropertyId(key))(key => getVertexFieldId(key))

  private def getEdgeFieldId(key: String) =
    _rap.getEdgeFieldId(key.toLowerCase())

  private def getEdgePropertyId(key: String) =
    _rap.getEdgePropertyId(key.toLowerCase())

  private def getVertexFieldId(key: String) =
    _rap.getVertexFieldId(key.toLowerCase())

  private def getVertexPropertyId(key: String) =
    _rap.getVertexPropertyId(key.toLowerCase())

  private def setProps(e: Entity, msgTime: Long, properties: Properties)(
      lookupProp: String => Int
  )(lookupField: String => Int): Unit =
    properties.properties.foreach {
      case ImmutableString(key, value) =>
        val FIELD    = lookupField(key.toLowerCase())
        val accessor: EntityFieldAccessor = e.getField(FIELD)
        accessor.set(new java.lang.StringBuilder(value))
      case MutableString(key, value)   =>
        val FIELD = lookupProp(key)
        e.getProperty(FIELD).setHistory(true, msgTime).set(new java.lang.StringBuilder(value))
      case MutableLong(key, value)     =>
        val FIELD = lookupProp(key)
        if (!e.getProperty(FIELD).isSet)
          e.getProperty(FIELD).setHistory(true, msgTime).set(value)
        else {
          val accessor = e.getProperty(FIELD)
          accessor.setHistory(false, msgTime).set(value)
          e match {
            case _: Edge   =>
              _rap.getEdgeMgr.addProperty(e.getLocalId, FIELD, accessor)
            case _: Vertex =>
              _rap.getVertexMgr.addProperty(e.getLocalId, FIELD, accessor)
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

}

case class QueuePayload(var graphUpdate: GraphUpdate)
