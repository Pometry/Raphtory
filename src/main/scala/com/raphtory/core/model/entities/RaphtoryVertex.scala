package com.raphtory.core.model.entities

import com.raphtory.core.analysis.GraphLens
import com.raphtory.core.analysis.entity.{Edge, Vertex}
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

  def viewAt(time: Long,lens:GraphLens): Vertex = {
    Vertex(this,
      incomingEdges.collect {
        case (k, edge) if edge.aliveAt(time) =>
          k -> Edge(edge, k, lens)
      },
      outgoingEdges.collect {
        case (k, edge) if edge.aliveAt(time) =>
          k -> Edge(edge, k, lens)
      },
      lens)
  }

  def viewAtWithWindow(time: Long, windowSize: Long,lens:GraphLens): Vertex = {
    Vertex(this,
      incomingEdges.collect {
        case (k, edge) if edge.aliveAtWithWindow(time,windowSize) =>
          k -> Edge(edge, k, lens)
      },
      outgoingEdges.collect {
        case (k, edge) if edge.aliveAtWithWindow(time,windowSize) =>
          k -> Edge(edge, k, lens)
      },
      lens)
  }


}
