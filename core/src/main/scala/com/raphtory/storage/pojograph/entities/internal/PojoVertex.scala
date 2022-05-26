package com.raphtory.storage.pojograph.entities.internal

import com.raphtory.graph.visitor.Vertex
import com.raphtory.storage.pojograph.PojoGraphLens
import com.raphtory.storage.pojograph.entities.external.PojoExEdge
import com.raphtory.storage.pojograph.entities.external.PojoExVertex

import scala.collection.mutable

/** @note DoNotDocument */
class PojoVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, initialValue) {

  var incomingEdges: mutable.Map[Long, PojoEdge] =
    mutable.Map[Long, PojoEdge]() //Map of all edges associated with the vertex
  var outgoingEdges: mutable.Map[Long, PojoEdge] = mutable.Map[Long, PojoEdge]()

  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync()         = edgesRequiringSync += 1
  def getEdgesRequringSync()                = edgesRequiringSync
  def addIncomingEdge(edge: PojoEdge): Unit = incomingEdges.put(edge.getSrcId, edge)
  def addOutgoingEdge(edge: PojoEdge): Unit = outgoingEdges.put(edge.getDstId, edge)

  def addAssociatedEdge(edge: PojoEdge): Unit     =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id: Long): Option[PojoEdge] = outgoingEdges.get(id)
  def getIncomingEdge(id: Long): Option[PojoEdge] = incomingEdges.get(id)

  def viewBetween(startTime: Long, endTime: Long, lens: PojoGraphLens): PojoExVertex =
    new PojoExVertex(
            this,
            incomingEdges.collect {
              case (k, edge) if edge.aliveBetween(startTime, endTime) =>
                k -> new PojoExEdge(edge, k, lens)
            },
            outgoingEdges.collect {
              case (k, edge) if edge.aliveBetween(startTime, endTime) =>
                k -> new PojoExEdge(edge, k, lens)
            },
            lens
    )
}
