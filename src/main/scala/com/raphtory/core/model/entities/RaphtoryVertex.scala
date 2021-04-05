package com.raphtory.core.model.entities

import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication.VertexMultiQueue

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/** Companion Vertex object (extended creator for storage loads) */
object RaphtoryVertex {
  def apply(
      creationTime: Long,
      vertexId: Int,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property],
      storage: EntityStorage
  ) = {
    val v = new RaphtoryVertex(creationTime, vertexId, initialValue = true)
    v.history = previousState
    //v.associatedEdges = associatedEdges
    v.properties = properties
    v
  }

}

class RaphtoryVertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends RaphtoryEntity(msgTime, initialValue) {


  var incomingEdges = ParTrieMap[Long, RaphtoryEdge]() //Map of all edges associated with the vertex
  var outgoingEdges = ParTrieMap[Long, RaphtoryEdge]()
  var incomingProcessing = incomingEdges //Map of edges for the current view of the vertex
  var outgoingProcessing = outgoingEdges
  private var edgesRequiringSync = 0

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync()  =edgesRequiringSync+=1
  def getEdgesRequringSync() = edgesRequiringSync
  def addIncomingEdge(edge: RaphtoryEdge): Unit = incomingEdges.put(edge.getSrcId, edge)
  def addOutgoingEdge(edge: RaphtoryEdge): Unit = outgoingEdges.put(edge.getDstId, edge)
  def addAssociatedEdge(edge: RaphtoryEdge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id: Long): Option[RaphtoryEdge] = outgoingEdges.get(id)
  def getIncomingEdge(id: Long): Option[RaphtoryEdge] = incomingEdges.get(id)

  def viewAt(time: Long): RaphtoryVertex = {
    incomingProcessing = incomingEdges.filter(e => e._2.aliveAt(time))
    outgoingProcessing = outgoingEdges.filter(e => e._2.aliveAt(time))
    this
  }

  def viewAtWithWindow(time: Long, windowSize: Long): RaphtoryVertex = {
    incomingProcessing = incomingEdges.filter(e => e._2.aliveAtWithWindow(time, windowSize))
    outgoingProcessing = outgoingEdges.filter(e => e._2.aliveAtWithWindow(time, windowSize))
    this
  }

  override def equals(obj: scala.Any): Boolean =
    if (obj.isInstanceOf[RaphtoryVertex]) {
      val v2 = obj.asInstanceOf[RaphtoryVertex] //add associated edges
      if (!(vertexId == v2.vertexId) ||
          !(history.equals(v2.history)) ||
          !(oldestPoint == v2.oldestPoint) ||
          !(newestPoint == newestPoint) ||
          !(properties.equals(v2.properties)) ||
          !(incomingEdges.equals(v2.incomingEdges)) ||
          !(outgoingEdges.equals(v2.outgoingEdges)))
        false
      else true
    } else false

}
