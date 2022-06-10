package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.PojoExEdge
import com.raphtory.internals.storage.pojograph.entities.external.PojoExVertex

import scala.collection.mutable

private[raphtory] class PojoVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, initialValue) {

  private val intoE: mutable.Map[Long, PojoEdge] =
    mutable.Map[Long, PojoEdge]() //Map of all edges associated with the vertex
  private val outE: mutable.Map[Long, PojoEdge] = mutable.Map[Long, PojoEdge]()

  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync() = edgesRequiringSync += 1

  def getEdgesRequringSync() = edgesRequiringSync

  def addIncomingEdge(edge: PojoEdge): Unit = intoE.put(edge.getSrcId, edge)

  def addOutgoingEdge(edge: PojoEdge): Unit = outE.put(edge.getDstId, edge)

  def addAssociatedEdge(edge: PojoEdge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)

  def getOutgoingEdge(id: Long): Option[PojoEdge] = outE.get(id)

  def getIncomingEdge(id: Long): Option[PojoEdge] = intoE.get(id)

  def viewBetween(startTime: Long, endTime: Long, lens: PojoGraphLens): PojoExVertex =
    new PojoExVertex(
            this,
            intoE.collect(edgeView(startTime, endTime, lens)),
            outE.collect(edgeView(startTime, endTime, lens)),
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

  def outgoingEdges: Iterator[(Long, PojoEdge)] = outE.iterator

  def incomingEdges: Iterator[(Long, PojoEdge)] = intoE.iterator
}
