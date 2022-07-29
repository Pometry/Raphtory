package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.edge.PojoExEdge
import com.raphtory.internals.storage.pojograph.entities.external.vertex.PojoExVertex

import scala.collection.mutable

private[raphtory] class PojoVertex(msgTime: Long, index: Long, val vertexId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, index, initialValue) {

  private val intoEdges: mutable.Map[Long, PojoEdge] =
    mutable.Map[Long, PojoEdge]() //Map of all edges associated with the vertex
  private val outEdges: mutable.Map[Long, PojoEdge] = mutable.Map[Long, PojoEdge]()

  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync() = edgesRequiringSync += 1

  def getEdgesRequringSync() = edgesRequiringSync

  def addIncomingEdge(edge: PojoEdge): Unit = intoEdges.put(edge.getSrcId, edge)

  def addOutgoingEdge(edge: PojoEdge): Unit = outEdges.put(edge.getDstId, edge)

  def addAssociatedEdge(edge: PojoEdge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)

  def getOutgoingEdge(id: Long): Option[PojoEdge] = outEdges.get(id)

  def getIncomingEdge(id: Long): Option[PojoEdge] = intoEdges.get(id)

  def viewBetween(startTime: Long, endTime: Long, lens: PojoGraphLens): PojoExVertex =
    new PojoExVertex(
            this,
            intoEdges.collect(edgeView(startTime, endTime, lens)),
            outEdges.collect(edgeView(startTime, endTime, lens)),
            lens
    )

  @inline
  def edgeView(
      startTime: Long,
      endTime: Long,
      lens: PojoGraphLens
  ): PartialFunction[(Long, PojoEdge), (Long, PojoExEdge)] = {
    case (k, edge) if edge.aliveBetween(startTime, endTime) =>
      k -> new PojoExEdge(edge, k, lens)
  }

  def outgoingEdges: Iterator[(Long, PojoEdge)] = outEdges.iterator

  def incomingEdges: Iterator[(Long, PojoEdge)] = intoEdges.iterator
}
