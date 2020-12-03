package com.raphtory.core.model.graphentities

import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication.VertexMutliQueue

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/** Companion Vertex object (extended creator for storage loads) */
object Vertex {
  def apply(
      creationTime: Long,
      vertexId: Int,
      previousState: mutable.TreeMap[Long, Boolean],
      properties: ParTrieMap[String, Property],
      storage: EntityStorage
  ) = {
    val v = new Vertex(creationTime, vertexId, initialValue = true)
    v.history = previousState
    //v.associatedEdges = associatedEdges
    v.properties = properties
    v
  }

}

class Vertex(msgTime: Long, val vertexId: Long, initialValue: Boolean)
        extends Entity(msgTime, initialValue) {


  var incomingEdges = ParTrieMap[Long, Edge]() //Map of all edges associated with the vertex
  var outgoingEdges = ParTrieMap[Long, Edge]()
  var incomingProcessing = incomingEdges //Map of edges for the current view of the vertex
  var outgoingProcessing = outgoingEdges
  private var edgesRequiringSync = 0

  var multiQueue        = new VertexMutliQueue()    //Map of queues for all ongoing processing
  var computationValues = ParTrieMap[String, Any]() //Partial results kept between supersteps in calculation

  //Functions for adding associated edges to this vertex
  def incrementEdgesRequiringSync()  =edgesRequiringSync+=1
  def getEdgesRequringSync() =edgesRequiringSync
  def addIncomingEdge(edge: Edge): Unit = incomingEdges.put(edge.getSrcId, edge)
  def addOutgoingEdge(edge: Edge): Unit = outgoingEdges.put(edge.getDstId, edge)
  def addAssociatedEdge(edge: Edge): Unit =
    if (edge.getSrcId == vertexId) addOutgoingEdge(edge) else addIncomingEdge(edge)
  def getOutgoingEdge(id: Long): Option[Edge] = outgoingEdges.get(id)
  def getIncomingEdge(id: Long): Option[Edge] = incomingEdges.get(id)

  //Getters and setters for processing results
  def addCompValue(key: String, value: Any): Unit = computationValues += ((key, value))
  def containsCompvalue(key: String): Boolean     = computationValues contains (key)
  def getCompValue(key: String)                   = computationValues(key)
  def getOrSet(key: String, value: Any) =
    if (computationValues.contains(key))
      computationValues(key)
    else {
      computationValues += ((key, value))
      value
    }

  def viewAt(time: Long): Vertex = {
    incomingProcessing = incomingEdges.filter(e => e._2.aliveAt(time))
    outgoingProcessing = outgoingEdges.filter(e => e._2.aliveAt(time))
    this
  }

  def viewAtWithWindow(time: Long, windowSize: Long): Vertex = {
    incomingProcessing = incomingEdges.filter(e => e._2.aliveAtWithWindow(time, windowSize))
    outgoingProcessing = outgoingEdges.filter(e => e._2.aliveAtWithWindow(time, windowSize))
    this
  }

  override def equals(obj: scala.Any): Boolean =
    if (obj.isInstanceOf[Vertex]) {
      val v2 = obj.asInstanceOf[Vertex] //add associated edges
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
